/*
 * mcswap.c - swapping over RDMA driver
 *
 * swapmon is a backend for frontswap that intercepts pages in the
 * process of being swapped out to disk and attempts to send them
 * to a remote memory server through Infiniband. The pages are written
 * to a set of memory servers through multicast and are later read
 * through RDMA read operation.
 *
 * Copyright (C) 2021 Dimitris Lekkas <dilekkas@student.net.ethz.ch>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 */

#define pr_fmt(fmt) KBUILD_MODNAME ": " fmt

#include <linux/module.h>
#include <linux/frontswap.h>
#include <linux/vmalloc.h>
#include <linux/page-flags.h>
#include <linux/memcontrol.h>
#include <linux/smp.h>
#include <linux/inet.h>
#include <linux/delay.h>
#include <linux/version.h>
#include <linux/pagemap.h>

#include <rdma/ib_verbs.h>
#include <rdma/rdma_cm.h>
#include <rdma/ib_mad.h>
#include <rdma/ib.h>

#include <linux/init.h>
#include <linux/hashtable.h>
#include <linux/rbtree.h>

#define MCSWAP_RDMA_CONNECTION_TIMEOUT_MS 3000
#define MCSWAP_MAX_SEND_RETRIES 3
#define MCSWAP_PAGE_ACK_TIMEOUT_MS 1

#define MCSWAP_MCAST_JOIN_TIMEOUT_SEC 5
#define MCSWAP_LOAD_PAGE_TIMEOUT_SEC 3
#define MCSWAP_MAX_MCAST_CQE 12288

#ifdef CONFIG_DEBUG_FS
#include <linux/debugfs.h>

static atomic_t mcswap_stored_pages = ATOMIC_INIT(0);
static atomic_t mcswap_loaded_pages = ATOMIC_INIT(0);
static u64 mcswap_store_timeouts = 0;
static u64 mcswap_sent_pages = 0;
static unsigned long mcswap_store_avg_ns= 0;

#define MAX_LOG_TIME_ENTRIES 120000
static u32 mcswap_store_times[MAX_LOG_TIME_ENTRIES];
static u32 mcswap_load_times[MAX_LOG_TIME_ENTRIES];

static struct dentry *mcswap_debugfs_root;

static int __init mcswap_debugfs_init(void) {
	if (!debugfs_initialized()) {
		return -ENODEV;
	}

	mcswap_debugfs_root = debugfs_create_dir("mcswap", NULL);
	if (!mcswap_debugfs_root) {
		return -ENOMEM;
	}

	debugfs_create_atomic_t("stored_pages", S_IRUGO,
			mcswap_debugfs_root, &mcswap_stored_pages);
	debugfs_create_atomic_t("loaded_pages", S_IRUGO,
			mcswap_debugfs_root, &mcswap_loaded_pages);
	debugfs_create_u64("store_timeouts", S_IRUGO,
			mcswap_debugfs_root, &mcswap_store_timeouts);
	debugfs_create_u64("sent_pages", S_IRUGO,
			mcswap_debugfs_root, &mcswap_sent_pages);
	debugfs_create_ulong("store_avg_ns", S_IRUGO,
			mcswap_debugfs_root, &mcswap_store_avg_ns);
	debugfs_create_u32_array("store_measure_us", S_IRUGO,
			mcswap_debugfs_root, &mcswap_store_times[0], MAX_LOG_TIME_ENTRIES);
	debugfs_create_u32_array("load_measure_us", S_IRUGO,
			mcswap_debugfs_root, &mcswap_load_times[0], MAX_LOG_TIME_ENTRIES);
	return 0;
}
#else
static int __init mcswap_debugfs_init(void) { return 0; }
static void __exit mcswap_debugfs_exit(void) { }
#endif

int page_cnt = 0;

static struct kmem_cache *mcswap_entry_cache;
static struct kmem_cache *page_ack_req_cache;
static struct kmem_cache *rdma_req_cache;

static DEFINE_MUTEX(swapmon_rdma_ctrl_mutex);

enum comm_ctrl_opcode {
	MCAST_MEMBERSHIP_NACK,
	MCAST_MEMBERSHIP_ACK
};

struct page_info {
	u64 remote_addr;
	u32 rkey;
};

struct mcswap_entry {
	struct rb_node node;
	struct page_info info;
	pgoff_t offset;
	u8 sidx;
};

struct mcswap_tree {
	struct rb_root root;
	spinlock_t lock;
};

static struct mcswap_tree *mcswap_trees[MAX_SWAPFILES];

struct rdma_req {
	struct ib_cqe cqe;
	struct completion done;
	struct page *page;
	u64 dma;
};

struct page_ack_req {
	struct ib_cqe cqe;
	struct mcswap_entry *entry;
	struct completion ack;
	unsigned swap_type;
};


static struct mcswap_entry* rb_find_page_remote_info(
		struct rb_root *root, pgoff_t offset) {
	struct rb_node *node = root->rb_node;
	struct mcswap_entry *entry;

	while (node) {
		entry = container_of(node, struct mcswap_entry, node);
		if (entry->offset > offset) {
			node = node->rb_left;
		} else if (entry->offset < offset) {
			node = node->rb_right;
		} else {
			return entry;
		}
	}
	return NULL;
}

static int mcswap_rb_insert(struct rb_root *root,
		struct mcswap_entry *data) {
	struct rb_node **new = &root->rb_node;
	struct rb_node *parent = NULL;
	struct mcswap_entry *entry;

	while (*new) {
		entry = container_of(*new, struct mcswap_entry, node);
		parent = *new;
		if (entry->offset > data->offset) {
			new = &((*new)->rb_left);
		} else if (entry->offset < data->offset) {
			new = &((*new)->rb_right);
		} else {
			return -EEXIST;
		}
	}

	// add new node and rebalance red-black tree
	rb_link_node(&data->node, parent, new);
	rb_insert_color(&data->node, root);
	return 0;
}

static int mcswap_rb_erase(struct rb_root *root, pgoff_t offset) {
	struct mcswap_entry *data;
	data = rb_find_page_remote_info(root, offset);
	if (data) {
		if (!RB_EMPTY_NODE(&data->node)) {
			rb_erase(&data->node, root);
			RB_CLEAR_NODE(&data->node);
			return 0;
		}
	}
	return -EEXIST;
}

struct mem_info {
	u64 addr;
	u32 len;
	u32 key;
};

#define mcswap_server_debug(msg, ctx)				\
	pr_info("server %s:%d %s\n", (ctx)->ip, (ctx)->port, msg)

#define mcswap_server_warn(msg, ctx)				\
	pr_warn("server %s:%d %s\n", (ctx)->ip, (ctx)->port, msg)

struct server_ctx {
	int sidx;

	struct rdma_cm_id *cm_id;
	struct ib_qp *qp;
	struct ib_pd *pd;

	struct ib_cq *send_cq;
	struct ib_cq *recv_cq;

	struct mem_info mem;
	struct sockaddr_in sin_addr;

	struct completion cm_done;
	struct completion mcast_ack;
	struct ib_cqe cqe;
	int cm_error;

	char *ip;
	int port;
};

struct mcswap_route_info {
	u8 gid_raw[16];
	u16 lid;
	u32 qkey;
};

struct mcast_ctx {
	struct mcswap_route_info *ri;

	struct rdma_cm_id *cm_id;
	struct ib_qp *qp;
	struct ib_cq *cq;
	struct ib_ah *ah;

	u32 qpn;
	u32 qkey;
};

struct swapmon_rdma_ctrl {
	struct ib_device *dev;
	struct ib_pd *pd;
	struct ib_cq *send_cq;

	struct mcast_ctx mcast;
	struct server_ctx *server;

	struct completion mcast_done;

	int cm_error;

	// IPv4 addresses
	struct sockaddr_in client_addr;

	// Infiniband address
	struct sockaddr_ib mcast_addr;

	atomic_t inflight_loads;

	int n_servers;
};

struct swapmon_rdma_ctrl* ctrl;


static void mcswap_fs_init(unsigned swap_type) {
	struct mcswap_tree *tree;
	pr_info("%s(%d)", __func__, swap_type);
	tree = kzalloc(sizeof(*tree), GFP_KERNEL);
	if (!tree) {
		pr_err("failed to alloc memory for red-black tree\n");
		return;
	}
	tree->root = RB_ROOT;
	spin_lock_init(&tree->lock);
	mcswap_trees[swap_type] = tree;
}

static void swapmon_mcast_done(struct ib_cq *cq, struct ib_wc *wc) {
	if (unlikely(wc->status != IB_WC_SUCCESS)) {
		pr_err("Polling failed with status: %s.\n",
					 ib_wc_status_msg(wc->status));
		return;
	}
}

static int mcswap_mcast_send_page(struct page *page, pgoff_t offset) {
	struct mcast_ctx *mc_ctx = &ctrl->mcast;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4,19,0)
	const struct ib_send_wr *bad_wr;
#else
	struct ib_send_wr *bad_wr;
#endif
	struct ib_ud_wr ud_wr;
	struct ib_sge sge;
	u64 dma_addr;
	int ret;

  dma_addr = ib_dma_map_page(ctrl->dev, page, 0,
			PAGE_SIZE, DMA_TO_DEVICE);
	if (unlikely(ib_dma_mapping_error(ctrl->dev, dma_addr))) {
		pr_err("%s: dma mapping error\n", __func__);
		return -1;
	}

	ib_dma_sync_single_for_device(ctrl->dev, dma_addr,
			PAGE_SIZE, DMA_TO_DEVICE);

	sge.addr = dma_addr;
	sge.length = PAGE_SIZE;
	sge.lkey = ctrl->pd->local_dma_lkey;

	ud_wr.wr.next = NULL;
	ud_wr.wr.wr_cqe = (struct ib_cqe *)
		kzalloc(sizeof(struct ib_cqe), GFP_KERNEL);
	if (!ud_wr.wr.wr_cqe) {
		pr_err("kzalloc() failed to allocate memory for ib_cqe.\n");
		return -ENOMEM;
	}
	ud_wr.wr.wr_cqe->done = swapmon_mcast_done;
	ud_wr.wr.sg_list = &sge;
	ud_wr.wr.num_sge = 1;
	ud_wr.wr.opcode = IB_WR_SEND_WITH_IMM;
	ud_wr.wr.send_flags = IB_SEND_SIGNALED;
	ud_wr.wr.ex.imm_data = htonl(offset);

	ud_wr.ah = mc_ctx->ah;
	ud_wr.remote_qpn = mc_ctx->qpn;
	ud_wr.remote_qkey = mc_ctx->qkey;

	ret = ib_post_send(mc_ctx->qp, &ud_wr.wr, &bad_wr);
	if (ret) {
		pr_err("ib_post_send failed to multicast page (error = %d).\n", ret);
		return ret;
	}

	return 0;
}

static void page_ack_done_sync(struct ib_cq *cq, struct ib_wc *wc) {
	struct page_ack_req *req;

	if (unlikely(wc->status != IB_WC_SUCCESS)) {
		if (wc->status == IB_WC_WR_FLUSH_ERR) {
			// TODO(dimlek): here we should probably signal the completion
			// and indicate somehow that
			pr_info("CQ was flushed.\n");
		} else {
			pr_err("Polling failed with status: %s.\n",
						 ib_wc_status_msg(wc->status));
		}
		return;
	}

	req = container_of(wc->wr_cqe, struct page_ack_req, cqe);
	complete(&req->ack);
}

static void page_ack_done(struct ib_cq *cq, struct ib_wc *wc) {
	struct mcswap_tree *tree;
	struct page_ack_req *req;
	struct mcswap_entry *ent;
	struct server_ctx *sctx;
	pgoff_t offset;
	int ret;

	if (unlikely(wc->status != IB_WC_SUCCESS)) {
		pr_err("Polling failed with status: %s.\n",
					 ib_wc_status_msg(wc->status));
		return;
	}
	if (unlikely(!(wc->wc_flags & IB_WC_WITH_IMM))) {
		pr_err("ACK message doesn't contain immediate data.\n");
		return;
	}
	req = container_of(wc->wr_cqe, struct page_ack_req, cqe);
	ent = req->entry;

	tree = mcswap_trees[req->swap_type];
	sctx = &ctrl->server[ent->sidx];

	offset = ntohl(wc->ex.imm_data);
	spin_lock_bh(&tree->lock);
	ret = mcswap_rb_insert(&tree->root, req->entry);
	if (ret) {
		// entry already exists for this offset, so we need to release
		// the lock and free the mcswap_entry structure here.
		spin_unlock_bh(&tree->lock);
		kmem_cache_free(mcswap_entry_cache, req->entry);
	}
#ifdef DEBUG
	else {
		// entry was added to the remote memory mapping
		ent = rb_find_page_remote_info(&tree->root, offset);
		spin_unlock_bh(&tree->lock);
		pr_info("[%u] page %lu stored at: remote addr = %llu, rkey = %u, "
				"server = %s:%d\n", atomic_read(&mcswap_stored_pages), offset,
				ent->info.remote_addr, ent->info.rkey, sctx->ip, sctx->port);
  }
#else
	spin_unlock_bh(&tree->lock);
#endif
	kmem_cache_free(page_ack_req_cache, req);
}


static int mcswap_post_recv_page_ack(struct page_ack_req *req) {
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4,19,0)
	const struct ib_recv_wr *bad_wr;
#else
	struct ib_recv_wr *bad_wr;
#endif
	struct ib_recv_wr wr;
	struct ib_sge sge;
	struct mcswap_entry *ent;
	u64 dma_addr;
	int i, ret;

	for (i = 0; i < ctrl->n_servers; i++) {
		ent = req[i].entry;
		dma_addr = ib_dma_map_single(ctrl->dev, &ent->info,
				sizeof(ent->info), DMA_FROM_DEVICE);
		ret = ib_dma_mapping_error(ctrl->dev, dma_addr);
		if (unlikely(ret)) {
			pr_err("dma address mapping error: %d\n", ret);
			return ret;
		}

		ib_dma_sync_single_for_device(ctrl->dev, dma_addr,
				sizeof(ent->info), DMA_FROM_DEVICE);

		sge.addr = dma_addr;
		sge.length = sizeof(ent->info);
		sge.lkey = ctrl->pd->local_dma_lkey;

		wr.wr_cqe = &req[i].cqe;
		wr.next = NULL;
		wr.sg_list = &sge;
		wr.num_sge = 1;

		ret = ib_post_recv(ctrl->server[i].qp, &wr, &bad_wr);
		if (ret) {
			pr_err("ib_post_recv failed to post RR for server %s:%d"
					"for page ack (code = %d)\n", ctrl->server[i].ip,
					ctrl->server[i].port, ret);
			return ret;
		}
	}

	return 0;
}


static int swapmon_store_sync(unsigned swap_type, pgoff_t offset,
												 struct page *page) {
	struct mcswap_tree *tree = mcswap_trees[swap_type];
	struct mcswap_entry* rmem_node;
	struct page_ack_req* req;
	long wait_ret;
	int retries = 0;
	int ret;

	while (retries < MCSWAP_MAX_SEND_RETRIES) {

		rmem_node = kmem_cache_alloc(mcswap_entry_cache, GFP_KERNEL);
		if (unlikely(!rmem_node)) {
			pr_err("kzalloc failed to allocate mem for u64.\n");
			return -ENOMEM;
		}
		rmem_node->offset = offset;

		req = kmem_cache_alloc(page_ack_req_cache, GFP_KERNEL);
		if (unlikely(!req)) {
			pr_err("slab allocator failed to allocate mem for page_ack_req.\n");
			return -ENOMEM;
		}
		req->entry = rmem_node;
		req->cqe.done = page_ack_done_sync;
		req->swap_type = swap_type;
		init_completion(&req->ack);

		// post RR for the ACK message to be received from mem server
		// ret = post_recv_page_ack_msg(req);
		if (ret) {
			pr_err("post_recv_page_ack_msg failed.\n");
			goto free_page_req;
		}

		// multicast the physical page to the memory servers
		ret = mcswap_mcast_send_page(page, offset);
		if (unlikely(ret)) {
			pr_err("mcswap_mcast_send_page() failed.\n");
			goto free_page_req;
		}
		mcswap_sent_pages++;

		// do some polling here instead of waiting for the req to be done

		wait_ret = wait_for_completion_interruptible_timeout(&req->ack,
				msecs_to_jiffies(MCSWAP_PAGE_ACK_TIMEOUT_MS) + 1);
		if (unlikely(wait_ret == -ERESTARTSYS)) {
			pr_err("interrupted on ack waiting.\n");
			return wait_ret;
		} else if (unlikely(wait_ret == 0)) {
			pr_err("timed out on ack waiting.\n");
			mcswap_store_timeouts++;
		} else {
			break;
		}
		retries++;
	}

	if (retries >= MCSWAP_MAX_SEND_RETRIES) {
		pr_err("failed to send message after %d retries.\n", retries);
		return -retries;
	}

	// once we are here we have successfully received an ACK from mem server
	spin_lock_bh(&tree->lock);
	ret = mcswap_rb_insert(&tree->root, req->entry);
	spin_unlock_bh(&tree->lock);
	if (ret) {
		// entry already exists for this offset, so we need to release
		// the lock and free the mcswap_entry structure here.
		kmem_cache_free(mcswap_entry_cache, req->entry);
	}

free_page_req:
	kmem_cache_free(page_ack_req_cache, req);
	return ret;
}

static int __mcswap_store_sync(unsigned swap_type, pgoff_t offset,
		struct page *page) {
	int ret;
#ifdef BENCH
	struct timespec ts_start, ts_end;
	int n_pages = atomic_read(&mcswap_stored_pages);
	long elapsed;
#endif
#ifdef DEBUG
	struct mcswap_tree *tree = mcswap_trees[swap_type];
	struct mcswap_entry *ent;
	pr_info("[%u] %s(swap_type = %u, offset = %lu)\n",
				 atomic_read(&mcswap_stored_pages), __func__,
				 swap_type, offset);
#endif

#ifdef BENCH
	getnstimeofday(&ts_start);
#endif
	ret = swapmon_store_sync(swap_type, offset, page);
	if (ret) {
		pr_err("failed to store page with offset = %lu\n", offset);
		return ret;
	}
	atomic_inc(&mcswap_stored_pages);
#ifdef BENCH
	getnstimeofday(&ts_end);
	elapsed = ts_end.tv_nsec - ts_start.tv_nsec;
	// iteratively calculate average to avoid overflow
	mcswap_store_avg_ns += (elapsed - mcswap_store_avg_ns) / (n_pages + 1);
	mcswap_store_times[n_pages % MAX_LOG_TIME_ENTRIES] = elapsed;
#endif

#ifdef DEBUG
	// entry was added to the remote memory mapping
	spin_lock_bh(&tree->lock);
	ent = rb_find_page_remote_info(&tree->root, offset);
	spin_unlock_bh(&tree->lock);
	pr_info("[%u] page %lu stored at: remote addr = %llu, rkey = %u\n",
			atomic_read(&mcswap_stored_pages), offset, ent->info.remote_addr,
			ent->info.rkey);
#endif
	return ret;
}


static int mcswap_store_async(unsigned swap_type, pgoff_t offset,
												 struct page *page) {
	struct mcswap_entry* ent;
	struct page_ack_req* req;
	int i, ret;
	if (PageTransHuge(page)) {
		return -EINVAL;
	}
#ifdef DEBUG
	pr_info("[%u] %s(swap_type = %u, offset = %lu)\n",
				 atomic_read(&mcswap_stored_pages), __func__,
				 swap_type, offset);
#endif

	ret = kmem_cache_alloc_bulk(mcswap_entry_cache, GFP_KERNEL,
			ctrl->n_servers, (void *) &ent);
	if (unlikely(ret != ctrl->n_servers)) {
		pr_err("kmem_cache_alloc_bulk failed: %d\n", ret);
		return -ENOMEM;
	}
	for (i = 0; i < ctrl->n_servers; i++) {
		ent[i].offset = offset;
		ent[i].sidx = i;
	}

	ret = kmem_cache_alloc_bulk(page_ack_req_cache, GFP_KERNEL,
			ctrl->n_servers, (void *) &req);
	if (unlikely(ret != ctrl->n_servers)) {
		pr_err("kmem_cache_alloc_bulk failed: %d\n", ret);
		return -ENOMEM;
	}
	for (i = 0; i < ctrl->n_servers; i++) {
		req[i].entry = &ent[i];
		req[i].cqe.done = page_ack_done;
		req[i].swap_type = swap_type;
	}

	// post RR for the ACK message to be received from mem server
	ret = mcswap_post_recv_page_ack(req);
	if (unlikely(ret)) {
		return ret;
	}
	// multicast the physical page to the memory servers
	ret = mcswap_mcast_send_page(page, offset);
	if (unlikely(ret)) {
		return ret;
	}
	atomic_inc(&mcswap_stored_pages);
	return 0;
}

static void page_read_done(struct ib_cq *cq, struct ib_wc *wc) {
	struct rdma_req *req = container_of(wc->wr_cqe, struct rdma_req, cqe);
	if (unlikely(wc->status != IB_WC_SUCCESS)) {
		pr_err("Polling failed with status: %s.\n",
					 ib_wc_status_msg(wc->status));
		return;
	}
	ib_dma_unmap_page(ctrl->dev, req->dma, PAGE_SIZE, DMA_BIDIRECTIONAL);
	atomic_dec(&ctrl->inflight_loads);
	complete(&req->done);

#ifdef ASYNC
	if (!PageUptodate(req->page)) {
		SetPageUptodate(req->page);
		unlock_page(req->page);
	}
#endif

	if (cq->poll_ctx == IB_POLL_DIRECT) {
		kmem_cache_free(rdma_req_cache, req);
	}
}

static int mcswap_rdma_read_sync(struct page *page,
		struct mcswap_entry *ent, struct server_ctx *sctx) {
	struct timespec ts_start, ts_now;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4,19,0)
	const struct ib_send_wr *bad_wr;
#else
	struct ib_send_wr *bad_wr;
#endif
	struct ib_rdma_wr rdma_wr;
	struct rdma_req *req;
	struct ib_sge sge;
	u64 dma_addr;
	long wait_ret = 0;
	int ret = 0;

  dma_addr = ib_dma_map_page(ctrl->dev, page, 0,
			PAGE_SIZE, DMA_BIDIRECTIONAL);
	ret = ib_dma_mapping_error(ctrl->dev, dma_addr);
	if (unlikely(ret)) {
		pr_err("(%s) dma mapping error %d\n", __func__, ret);
		return ret;
	}

	ib_dma_sync_single_for_device(ctrl->dev, dma_addr,
			PAGE_SIZE, DMA_BIDIRECTIONAL);

	sge.addr = dma_addr;
	sge.length = PAGE_SIZE;
	sge.lkey = ctrl->pd->local_dma_lkey;

	req = kmem_cache_alloc(rdma_req_cache, GFP_KERNEL);
	if (unlikely(!req)) {
		pr_err("slab allocator failed to allocate mem for rdma_req.\n");
		return -ENOMEM;
	}
	init_completion(&req->done);
	req->cqe.done = page_read_done;
	req->page = page;
	req->dma = dma_addr;

	rdma_wr.wr.wr_cqe = &req->cqe;
	rdma_wr.wr.next = NULL;
	rdma_wr.wr.sg_list = &sge;
	rdma_wr.wr.num_sge = 1;
	rdma_wr.wr.opcode = IB_WR_RDMA_READ;
	rdma_wr.wr.send_flags = IB_SEND_SIGNALED;

	rdma_wr.remote_addr = ent->info.remote_addr;
	rdma_wr.rkey = ent->info.rkey;

	ret = ib_post_send(sctx->qp, &rdma_wr.wr, &bad_wr);
	if (unlikely(ret)) {
		pr_err("ib_post_send failed to post RR for page (error = %d)\n", ret);
		return ret;
	}
	atomic_inc(&ctrl->inflight_loads);

	if (sctx->qp->send_cq->poll_ctx == IB_POLL_DIRECT) {
		getnstimeofday(&ts_start);
		while (atomic_read(&ctrl->inflight_loads) > 0) {
		  ib_process_cq_direct(sctx->qp->send_cq, 1);
			getnstimeofday(&ts_now);
			if (ts_now.tv_sec - ts_start.tv_sec >=
					MCSWAP_LOAD_PAGE_TIMEOUT_SEC) {
				pr_err("timed out when reading page %lu\n", ent->offset);
				return -ETIME;
			}
			cpu_relax();
		}
		return ret;
	}

	wait_ret = wait_for_completion_interruptible_timeout(&req->done,
			msecs_to_jiffies(MCSWAP_PAGE_ACK_TIMEOUT_MS) + 1);
	if (unlikely(wait_ret == -ERESTARTSYS)) {
		pr_err("interrupted on ack waiting.\n");
		return wait_ret;
	} else if (unlikely(wait_ret == 0)) {
		pr_err("timed out on ack waiting.\n");
		return -ETIME;
	}

	kmem_cache_free(rdma_req_cache, req);
	return ret;
}



static int __mcswap_load_sync(unsigned swap_type, pgoff_t offset,
		struct page *page) {
	struct mcswap_tree *tree = mcswap_trees[swap_type];
	struct server_ctx *sctx;
	struct mcswap_entry *entry;
	int ret;

#ifdef DEBUG
	pr_info("[%u] %s(swap_type = %u, offset = %lu)\n",
				 atomic_read(&mcswap_loaded_pages), __func__,
				 swap_type, offset);
#endif

	spin_lock_bh(&tree->lock);
	entry = rb_find_page_remote_info(&tree->root, offset);
	if (!entry) {
		pr_err("failed to find page metadata in red-black tree\n");
		spin_unlock_bh(&tree->lock);
		return -EEXIST;
	}
	spin_unlock_bh(&tree->lock);
	sctx = &ctrl->server[entry->sidx];

	ret = mcswap_rdma_read_sync(page, entry, sctx);
	if (unlikely(ret)) {
		pr_info("failed to fetch remote page %lu from server %s:%d\n",
				offset, sctx->ip, sctx->port);
		return ret;
	}

#ifdef DEBUG
	pr_info("[%u] page %lu loaded from: remote addr = %llu, rkey = %u "
			"server = %s:%d\n", atomic_read(&mcswap_loaded_pages), offset,
			entry->info.remote_addr, entry->info.rkey, sctx->ip, sctx->port);
#endif

	return 0;
}

static int mcswap_load_sync(unsigned swap_type, pgoff_t offset,
		struct page *page) {
	int ret;
#ifdef BENCH
	struct timespec ts_start, ts_end;
	int n_pages = atomic_read(&mcswap_loaded_pages);
	long elapsed;
#endif
#ifdef BENCH
	getnstimeofday(&ts_start);
#endif
	ret = __mcswap_load_sync(swap_type, offset, page);
	if (unlikely(ret)) {
		pr_info("failed to load page with offset = %lu\n", offset);
		return ret;
	}
	atomic_inc(&mcswap_loaded_pages);
#ifdef BENCH
	getnstimeofday(&ts_end);
	elapsed = ts_end.tv_nsec - ts_start.tv_nsec;
	mcswap_load_times[n_pages % MAX_LOG_TIME_ENTRIES] = elapsed;
#endif
	return 0;
}


static void mcswap_invalidate_page(unsigned swap_type, pgoff_t offset) {
	struct mcswap_tree *tree = mcswap_trees[swap_type];
	int ret;
#ifdef DEBUG
	pr_info("invalidate_page(swap_type = %u, offset = %lu)\n",
				 swap_type, offset);
#endif
	spin_lock_bh(&tree->lock);
	ret = mcswap_rb_erase(&tree->root, offset);
	if (unlikely(ret)) {
		pr_err("failed to erase page with offset = %lu\n", offset);
		return;
	}
	spin_unlock_bh(&tree->lock);
}

static void mcswap_invalidate_area(unsigned swap_type) {
	struct mcswap_tree *tree = mcswap_trees[swap_type];
	struct mcswap_entry *curr, *tmp;
#ifdef DEBUG
	pr_info("invalidate_area(swap_type = %u)\n", swap_type);
#endif
	spin_lock_bh(&tree->lock);
	rbtree_postorder_for_each_entry_safe(curr, tmp, &tree->root, node) {
		// TODO(dimlek): the check for empty node might not be needed here.
		if (!RB_EMPTY_NODE(&curr->node)) {
			rb_erase(&curr->node, &tree->root);
			kmem_cache_free(mcswap_entry_cache, curr);
			RB_CLEAR_NODE(&curr->node);
		}
	}
	spin_unlock_bh(&tree->lock);
	kfree(tree);
	mcswap_trees[swap_type] = NULL;
}

#ifdef ASYNC
static int mcswap_poll_load(int cpu) {
	return 0;
}
#endif

static struct frontswap_ops mcswap_sync_ops = {
	// prepares the device to receive frontswap pages associated with the
	// specified swap device number (aka "type"). This function is invoked
	// when we `swapon` a device.
	.init  = mcswap_fs_init,
	// copies the page to transcendent memory and associate it with the type
	// and offset associated with the page.
	.store = __mcswap_store_sync,
	// copies the page, if found, from transcendent memory into kernel memory,
	// but will not remove the page from transcendent memory.
	.load  = mcswap_load_sync,
	// removes the page from transcendent memory.
	.invalidate_page = mcswap_invalidate_page,
	// removes all pages from transcendent memory.
	.invalidate_area = mcswap_invalidate_area,
};

static struct frontswap_ops mcswap_async_ops = {
	.init = mcswap_fs_init,
	.store = mcswap_store_async,
	.load = mcswap_load_sync,
#ifdef ASYNC
	.load_async = mcswap_load_sync,
	.poll_load = mcswap_poll_load,
#endif
	.invalidate_page = mcswap_invalidate_page,
	.invalidate_area = mcswap_invalidate_area
};


/*--------------------- Module parameters ---------------------*/

#define MAX_NR_SERVERS 3

static char *endpoint[MAX_NR_SERVERS];
static int nr_servers;
module_param_array(endpoint, charp, &nr_servers, 0644);
MODULE_PARM_DESC(endpoint, "IP address and port number of memory"
		"servers in n.n.n.n:n form");

static int server_port;
module_param_named(sport, server_port, int, 0644);
MODULE_PARM_DESC(server_port, "Port number that memory servers"
		" are listening on");

static char client_ip[INET_ADDRSTRLEN];
module_param_string(cip, client_ip, INET_ADDRSTRLEN, 0644);
MODULE_PARM_DESC(client_ip, "IP address in n.n.n.n form of the interface used"
		"for communicating with the memory servers");

static int enable_async_mode = 0;
module_param(enable_async_mode, int, 0644);
MODULE_PARM_DESC(enable_async_mode, "Enable asynchronous stores/loads "
		"but requires appropriate kernel patch (default=0)");

static int enable_poll_mode = 0;
module_param(enable_poll_mode, int, 0644);

/*-------------------------------------------------------------*/


static int mcswap_parse_ip_addr(struct sockaddr_in *saddr, char *ip) {
  u8 *addr = (u8 *) &saddr->sin_addr.s_addr;
  size_t buflen = strlen(ip);
	int ret;

	if (buflen > INET_ADDRSTRLEN) {
    return -EINVAL;
	}
	ret = in4_pton(ip, buflen, addr, '\0', NULL);
  if (ret <= 0) {
		printk("in4_pton() failed, unable to parse ip address: %s\n", ip);
    return -EINVAL;
	}
  saddr->sin_family = AF_INET;

  return 0;
}

static void swapmon_rdma_qp_event(struct ib_event *event, void *context) {
	pr_debug("QP event %s (%d)\n", ib_event_msg(event->event), event->event);
}

// In order to perform RDMA operations, establishment of a connection to
// the remote host as well as appropriate permissions need to be set up first.
// The mechanism for accomplishing this is the Queue Pair (QP) which is roughly
// equivalent to a socket. The QP needs to be initialized on both sides of the
// connection. Once a QP is established, the verbs API can be used to perform
// RDMA reads/writes and atomic ops. Transport layer communication is done
// between a QP in each communicating HCA port.
//
// Each QP is made up of a Send Queue (SQ) (used for sending outgoing messages),
// a Receive Queue (RQ) (used for receiving incoming messages), and their
// associated Completion Queues (CQs). A "queue" is analogous to a FIFO waiting
// line where items must leave the queue in the same order that they enter it.
static int mcswap_rc_create_qp(struct server_ctx *ctx) {
	struct ib_qp_init_attr init_attr;
	int ret;

	memset(&init_attr, 0, sizeof(init_attr));
	init_attr.event_handler = swapmon_rdma_qp_event;
	init_attr.cap.max_send_wr = 8096;
	init_attr.cap.max_recv_wr = 8096;
	init_attr.cap.max_recv_sge = 1;
	init_attr.cap.max_send_sge = 1;
	init_attr.sq_sig_type = IB_SIGNAL_REQ_WR;
	init_attr.qp_type = IB_QPT_RC;
	init_attr.send_cq = ctrl->send_cq;
	init_attr.recv_cq = ctx->recv_cq;

	// allocate a queue pair (QP) and associate it with the specified
	// RDMA identifier cm_id
	ret = rdma_create_qp(ctx->cm_id, ctx->pd, &init_attr);
	if (ret) {
		pr_err("rdma_create_qp failed: %d\n", ret);
		return ret;
	}
	ctx->qp = ctx->cm_id->qp;
	return ret;
}

static int mcswap_rdma_addr_resolved(struct rdma_cm_id *cm_id) {
	struct server_ctx *ctx = cm_id->context;
	enum ib_poll_context poll_ctx;
	int comp_vector = 0;
	int ret;

	// struct ib_device represents a handle to the HCA
	if (!ctrl->dev) {
		pr_err("no rdma device found.\n");
		ret = -ENODEV;
		goto out_err;
	}

	// Allocate a protection domain which we can use to create objects within
	// the domain. we can register and associate memory regions with this PD
	// In addition to the CQs to be associated with the QP about to be created,
	// the PD that the QP will belong to must also have been created.
	if (ctrl->pd) {
		ctx->pd = ctrl->pd;
	} else {
		pr_err("protection domain is invalid");
		ret = -ENOENT;
		goto out_err;
	}

	poll_ctx = enable_poll_mode ? IB_POLL_DIRECT : IB_POLL_SOFTIRQ;
	ctx->send_cq = ib_alloc_cq(ctrl->dev, ctx, 8096,
			comp_vector, poll_ctx);
	if (IS_ERR(ctx->send_cq)) {
		ret = PTR_ERR(ctx->send_cq);
		goto out_err;
	}

	// Before creating the local QP, we must first cause the HCA to create the CQs
	// to be associated with the SQ and RQ of the QP about to be created. The QP's
	// SQ and RQ can have separate CQs or may share a CQ. The 3rd argument
	// specifies the number of Completion Queue Entries (CQEs) and represents the
	// size of the CQ.
	ctx->recv_cq = ib_alloc_cq(ctrl->dev, ctx, 8096,
			comp_vector, IB_POLL_SOFTIRQ);
	if (IS_ERR(ctx->recv_cq)) {
		ret = PTR_ERR(ctx->recv_cq);
		goto out_destroy_send_cq;
	}

	ret = mcswap_rc_create_qp(ctx);
	if (ret) {
		goto out_destroy_recv_cq;
	}

	// we now resolve the RDMA address bound to the RDMA identifier into
	// route information needed to establish a connection. This is invoked
	// in the client side of the connection and we must have already resolved
	// the dest address into an RDMA address through `rdma_resolve_addr`.
	ret = rdma_resolve_route(ctx->cm_id,
			MCSWAP_RDMA_CONNECTION_TIMEOUT_MS);
	if (ret) {
		pr_err("rdma_resolve_route failed: %d\n", ret);
		goto out_destroy_qp;
	}
	return 0;

out_destroy_qp:
	rdma_destroy_qp(ctx->cm_id);
out_destroy_recv_cq:
	ib_free_cq(ctx->recv_cq);
out_destroy_send_cq:
	ib_free_cq(ctx->send_cq);
out_err:
	return ret;
}


static int mcswap_rdma_route_resolved(struct server_ctx *ctx) {
	struct mcast_ctx *mctx = &ctrl->mcast;
	struct rdma_conn_param param = {};
  int ret;

  param.private_data = (void *) mctx->ri;
  param.private_data_len = sizeof *mctx->ri;
  param.responder_resources = 8;
  param.initiator_depth = 8;
  param.flow_control = 0;
  param.retry_count = MCSWAP_MAX_SEND_RETRIES;
  param.rnr_retry_count = MCSWAP_MAX_SEND_RETRIES;
  param.qp_num = ctx->qp->qp_num;

	// Initiate an active connection request (i.e. send REQ message). The route
	// must have been resolved before trying to connect.
  ret = rdma_connect(ctx->cm_id, &param);
  if (ret) {
    pr_err("rdma_connect failed: %d\n", ret);
		ib_free_cq(ctx->send_cq);
    ib_free_cq(ctx->recv_cq);
		return ret;
  }
  return 0;
}

static int str_to_ib_sockaddr(char *dst, struct sockaddr_ib *mcast_addr) {
	int ret;
	size_t len = strlen(dst);
	if (len > INET6_ADDRSTRLEN) {
		return -EINVAL;
	}

	ret = in6_pton(dst, len, mcast_addr->sib_addr.sib_raw, '\0', NULL);
	if (ret <= 0) {
		pr_err("in6_pton() failed with ret = %d\n", ret);
		return ret;
	}
	mcast_addr->sib_family = AF_IB;
	return 0;
}


static int mcswap_mcast_join_handler(struct rdma_cm_event *event,
		struct mcast_ctx *mc_ctx) {
	struct rdma_ud_param *param = &event->param.ud;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(4,15,0)
	const struct ib_global_route *grh = rdma_ah_read_grh(&param->ah_attr);
	pr_info("IB multicast group info: dgid = %16phC, mlid = 0x%x, "
			"sl = %d, src_path_bits = %u, qpn = %u, qkey = %u\n",
			param->ah_attr.grh.dgid.raw, param->ah_attr.ib.dlid,
			rdma_ah_get_sl(&param->ah_attr), rdma_ah_get_path_bits(&param->ah_attr),
			param->qp_num, param->qkey);
#else
	const struct ib_global_route *grh = &param->ah_attr.grh;
#endif

	mc_ctx->ri = kzalloc(sizeof *mc_ctx->ri , GFP_KERNEL);
	if (!mc_ctx->ri) {
		pr_err("failed to alloc memory for route info\n");
		return -ENOMEM;
	}

	memcpy(mc_ctx->ri->gid_raw, grh->dgid.raw, sizeof grh->dgid);
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4,15,0)
	mc_ctx->ri->lid  = rdma_ah_get_dlid(&param->ah_attr);
#else
	mc_ctx->ri->lid = param->ah_attr.dlid;
#endif
	mc_ctx->ri->qkey = param->qkey;

	mc_ctx->qpn  = param->qp_num;
	mc_ctx->qkey = param->qkey;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(5,0,0)
	mc_ctx->ah = rdma_create_ah(ctrl->pd, &param->ah_attr,
			RDMA_CREATE_AH_SLEEPABLE);
#elif LINUX_VERSION_CODE >= KERNEL_VERSION(4,15,0)
	mc_ctx->ah = rdma_create_ah(ctrl->pd, &param->ah_attr);
#else
	mc_ctx->ah = ib_create_ah(ctrl->pd, &param->ah_attr);
#endif
	if (!mc_ctx->ah) {
		pr_err("failed to create address handle for multicast\n");
		return -ENOENT;
	}

	return 0;
}

static int recv_mem_info(struct server_ctx *ctx, struct ib_qp *qp) {
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4,19,0)
	const struct ib_recv_wr *bad_wr;
#else
	struct ib_recv_wr *bad_wr;
#endif
	struct ib_recv_wr wr;
	struct ib_sge sge;
	struct ib_wc wc;
	u64 dma_addr;
	int ret;

	struct server_info {
		u64 addr;
		u32 len;
		u32 key;
		u32 qpn;
	};

	struct server_info *msg;
	msg = kzalloc(sizeof(struct server_info), GFP_KERNEL);
	if (!msg) {
		pr_err("kzalloc failed to allocate mem for struct server_info.\n");
		return -ENOMEM;
	}

	dma_addr = ib_dma_map_single(ctrl->dev, msg,
			sizeof(struct server_info), DMA_FROM_DEVICE);
	ret = ib_dma_mapping_error(ctrl->dev, dma_addr);
	if (ret) {
		pr_err("(%s) dma address mapping error %d\n", __func__, ret);
		return ret;
	}

	// Prepare DMA region to be accessed by device.
	ib_dma_sync_single_for_device(ctrl->dev, dma_addr,
			sizeof(struct server_info), DMA_FROM_DEVICE);

	sge.addr = dma_addr;
	sge.length = sizeof(struct server_info);
	sge.lkey = ctx->pd->local_dma_lkey;

	wr.next = NULL;
	wr.wr_id = 0;
	wr.sg_list = &sge;
	wr.num_sge = 1;

	ret = ib_post_recv(qp, &wr, &bad_wr);
	if (ret) {
		pr_err("(%s) ib_post_recv failed: %d\n", __func__, ret);
		return ret;
	}

	while (!ib_poll_cq(qp->recv_cq, 1, &wc)) {
		// nothing
	}

	if (wc.status != IB_WC_SUCCESS) {
		printk("Polling failed with status %s (work request ID = %llu).\n",
					 ib_wc_status_msg(wc.status), wc.wr_id);
		return wc.status;
	}

	ctx->mem.addr = msg->addr;
	ctx->mem.len  = msg->len;
	ctx->mem.key  = msg->key;
	pr_info("Remote mem info: addr = %llu, len = %u, key = %u\n",
			ctx->mem.addr, ctx->mem.len, ctx->mem.key);

	return 0;
}

static int mcswap_cm_event_handler(struct rdma_cm_id *id,
		struct rdma_cm_event *event) {
	struct server_ctx *ctx;
	struct mcast_ctx *mctx;
	int cm_error = 0;
	int ret;

	if (id->qp_type == IB_QPT_RC) {
		ctx = id->context;
		pr_debug("%s [%s:%d]: %s (%d) status %d id $%p\n", __func__,
				ctx->ip, ctx->port, rdma_event_msg(event->event),
				event->event, event->status, id);
	} else if (id->qp_type == IB_QPT_UD) {
		mctx = id->context;
		pr_debug("%s : %s (%d) status %d id $%p\n", __func__,
				rdma_event_msg(event->event), event->event,
				event->status, id);
	}

	switch (event->event) {
		case RDMA_CM_EVENT_ADDR_RESOLVED:
			cm_error = mcswap_rdma_addr_resolved(id);
			break;
		case RDMA_CM_EVENT_ROUTE_RESOLVED:
			cm_error = mcswap_rdma_route_resolved(id->context);
			break;
		case RDMA_CM_EVENT_ESTABLISHED:
			mcswap_server_debug("connected",
					(struct server_ctx*) id->context);
			// wait to receive memory info to retrieve all required
			// info to properly reference remote memory.
			ctx = id->context;
			ret = recv_mem_info(ctx, id->qp);
			if (ret) {
				// TODO(dimlek): here we don't want to exit with failure since
				// we should offer dual functionality and sending memory info
				// should be optional
				mcswap_server_debug("failed to receive memory info", ctx);
			}
			ctx->cm_error = ret;
			complete(&ctx->cm_done);
			break;
		case RDMA_CM_EVENT_ROUTE_ERROR:
		case RDMA_CM_EVENT_ADDR_ERROR:
		case RDMA_CM_EVENT_CONNECT_REQUEST:
		case RDMA_CM_EVENT_CONNECT_RESPONSE:
			break;
		case RDMA_CM_EVENT_CONNECT_ERROR:
			mcswap_server_warn("failed to connect",
					(struct server_ctx*) id->context);
			break;
		case RDMA_CM_EVENT_UNREACHABLE:
			break;
		case RDMA_CM_EVENT_REJECTED:
			break;
		case RDMA_CM_EVENT_DISCONNECTED:
			// TODO(dimlek): we need to do some cleanup of resources here
			mcswap_server_debug("disconnected",
					(struct server_ctx*) id->context);
			break;
		case RDMA_CM_EVENT_DEVICE_REMOVAL:
			break;
		case RDMA_CM_EVENT_MULTICAST_JOIN:
			pr_debug("multicast join operation completed successfully\n");
			cm_error = mcswap_mcast_join_handler(event, id->context);
			ctrl->cm_error = cm_error;
			complete(&ctrl->mcast_done);
			return 0;
		case RDMA_CM_EVENT_MULTICAST_ERROR:
			break;
		case RDMA_CM_EVENT_ADDR_CHANGE:
		case RDMA_CM_EVENT_TIMEWAIT_EXIT:
			break;
		default:
			pr_err("unrecognized RDMA CM event %d\n", event->event);
			break;
	}

	/*
	if (cm_error) {
		pr_info("cm_error=%d\n", cm_error);
		ctx = id->context;
		ctx->cm_error = cm_error;
		complete(&ctx->cm_done);
	}
	*/

	return 0;
}


static void recv_control_msg_done(struct ib_cq* cq, struct ib_wc* wc) {
	struct server_ctx *ctx;
	enum comm_ctrl_opcode op;
	if (unlikely(wc->status != IB_WC_SUCCESS)) {
		pr_err("control message WC failed with status: %s\n",
				ib_wc_status_msg(wc->status));
		return;
	}
	if (unlikely(!(wc->wc_flags & IB_WC_WITH_IMM))) {
		pr_err("control message doesn't contain IMM data\n");
		return;
	}

	ctx = container_of(wc->wr_cqe, struct server_ctx, cqe);
	op = ntohl(wc->ex.imm_data);
	switch(op) {
		case MCAST_MEMBERSHIP_ACK:
			mcswap_server_debug("joined multicast group", ctx);
			complete(&ctx->mcast_ack);
			break;
		case MCAST_MEMBERSHIP_NACK:
			mcswap_server_warn("failed to join multicast group", ctx);
			break;
		default:
			pr_err("(%s) unrecognized control message with op: %u\n",
					__func__, op);
			break;
	}
}

static int mcswap_post_recv_control_msg(struct server_ctx *ctx) {
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4,19,0)
	const struct ib_recv_wr *bad_wr;
#else
	struct ib_recv_wr *bad_wr;
#endif
	struct ib_recv_wr wr;
	int ret;

	wr.next    = NULL;
	wr.sg_list = NULL;
	wr.num_sge = 0;
	wr.wr_cqe = &ctx->cqe;
	wr.wr_cqe->done = recv_control_msg_done;

	ret = ib_post_recv(ctx->qp, &wr, &bad_wr);
	if (unlikely(ret)) {
		pr_info("(%s) ib_post_recv failed: %d\n", __func__, ret);
		return ret;
	}

	return 0;
}


static struct ib_qp* __init mcswap_create_mcast_qp(
		struct rdma_cm_id *cm_id, struct ib_pd *pd, struct ib_cq *cq) {
	struct ib_qp_init_attr init_attr;
	int ret;
	memset(&init_attr, 0, sizeof(init_attr));
	init_attr.event_handler = swapmon_rdma_qp_event;
	init_attr.sq_sig_type = IB_SIGNAL_REQ_WR;
	init_attr.qp_type = IB_QPT_UD;
	init_attr.cap.max_send_wr  = 12 * 1024;
	init_attr.cap.max_send_sge = 1;
	init_attr.cap.max_recv_wr  = 0;
	init_attr.cap.max_recv_sge = 0;
	init_attr.send_cq = cq;
	init_attr.recv_cq = cq;

	ret = rdma_create_qp(cm_id, pd, &init_attr);
	if (ret) {
		pr_err("failed to create multicast qp: %d\n", ret);
		return NULL;
	}
	return cm_id->qp;
}

static int mcswap_wait_for_mcast_join(void) {
	int wait_ret = wait_for_completion_interruptible_timeout(&ctrl->mcast_done,
			msecs_to_jiffies(MCSWAP_MCAST_JOIN_TIMEOUT_SEC * 1000) + 1);
	if (wait_ret == 0) {
		pr_err("timed out on multicast join waiting\n");
		return -ETIME;
	} else if (wait_ret == -ERESTARTSYS) {
		pr_err("interrupted on multicast join waiting\n");
		return wait_ret;
	}
	return 0;
}

static int __init mcswap_wait_for_rc_establishment(struct server_ctx *ctx) {
	int wait_ret = wait_for_completion_interruptible_timeout(&ctx->cm_done,
			msecs_to_jiffies(MCSWAP_RDMA_CONNECTION_TIMEOUT_MS) + 1);
	if (wait_ret == 0) {
		pr_err("connection establishment timeout\n");
		return -ETIME;
	} else if (wait_ret == -ERESTARTSYS) {
		pr_err("interrupted on connection establishment\n");
		return wait_ret;
	}
	return 0;
}

static int __init mcswap_wait_for_server_mcast_ack(struct server_ctx *ctx) {
	struct timespec ts_start, ts_now;
	int wait_ret;

	if (ctx->recv_cq->poll_ctx == IB_POLL_DIRECT) {
		getnstimeofday(&ts_start);
		while (!try_wait_for_completion(&ctx->mcast_ack)) {
		  ib_process_cq_direct(ctx->recv_cq, 1);
			getnstimeofday(&ts_now);
			if (ts_now.tv_sec - ts_start.tv_sec >=
					MCSWAP_MCAST_JOIN_TIMEOUT_SEC) {
				return -ETIME;
			}
			cpu_relax();
		}
		return 0;
	}

	wait_ret = wait_for_completion_interruptible_timeout(&ctx->mcast_ack,
			msecs_to_jiffies(MCSWAP_MCAST_JOIN_TIMEOUT_SEC * 1000) + 1);
	if (wait_ret == 0) {
		pr_err("server %s:%d multicast join timeout\n", ctx->ip, ctx->port);
		return -ETIME;
	} else if (wait_ret == -ERESTARTSYS) {
		pr_err("interrupted on server %s:%d multicast join waiting\n",
				ctx->ip, ctx->port);
		return wait_ret;
	}

	return 0;
}

static int mcswap_check_hca_support(struct ib_device *dev, u8 port_num) {
	struct ib_port_attr port_attr;
	int ret;

	ret = ib_query_port(dev, port_num, &port_attr);
	if (ret) {
		pr_err("ib_query_port failed (%d) to query port_num = %u\n",
				ret, port_num);
		return -ENODEV;
	}

	// ensure that memory registrations are supported
	if (!(dev->attrs.device_cap_flags & IB_DEVICE_MEM_MGT_EXTENSIONS)) {
		pr_err("memory registrations not supported on port = %u\n", port_num);
		return -EOPNOTSUPP;
	}

	// ensure that multicast is supported by the specified port of HCA
	if (!rdma_cap_ib_mcast(dev, port_num)) {
		pr_err("port %u of IB device doesn't support multicast\n", port_num);
		return -EOPNOTSUPP;
	}

	// ensure that the port we are using has an active MTU of 4096KB
	if (port_attr.active_mtu != IB_MTU_4096) {
		pr_err("port_num = %u needs active MTU = %u (current MTU = %u)\n",
				port_num, ib_mtu_enum_to_int(IB_MTU_4096),
				ib_mtu_enum_to_int(port_attr.active_mtu));
		return -EOPNOTSUPP;
	}

	return 0;
}

static int mcswap_rdma_mcast_init(struct mcast_ctx *mctx) {
	int comp_vector = 0;
	int ret;

	mctx->cm_id = rdma_create_id(&init_net, mcswap_cm_event_handler,
			mctx, RDMA_PS_UDP, IB_QPT_UD);
	if (IS_ERR(mctx->cm_id)) {
		pr_err("multicast cm id creation failed\n");
		return -ENODEV;
	}

	ret = rdma_bind_addr(mctx->cm_id, (struct sockaddr *)
			&ctrl->client_addr);
	if (ret) {
		pr_err("rdma_bind_addr failed: %d\n", ret);
		goto destroy_mcast_cm_id;
	}

	// since a specific local address was given (i.e. `client_addr`) to
	// the `rdma_bind_addr` call then the RDMA identifier will be bound
	// to the local RDMA device, so we can get a handle to the HCA
	ctrl->dev = mctx->cm_id->device;
	ret = mcswap_check_hca_support(ctrl->dev, mctx->cm_id->port_num);
	if (ret) {
		goto destroy_mcast_cm_id;
	}

	ctrl->pd = ib_alloc_pd(ctrl->dev, 0);
	if (IS_ERR(ctrl->pd)) {
		pr_err("failed to alloc protection domain\n");
		ret = PTR_ERR(ctrl->pd);
		goto destroy_mcast_cm_id;
	}

	mctx->cq = ib_alloc_cq(ctrl->dev, ctrl, MCSWAP_MAX_MCAST_CQE,
			comp_vector, IB_POLL_SOFTIRQ);
	if (IS_ERR(mctx->cq)) {
		pr_err("failed to alloc completion queue\n");
		ret = PTR_ERR(mctx->cq);
		goto destroy_mcast_pd;
	}

	mctx->qp = mcswap_create_mcast_qp(mctx->cm_id,
			ctrl->pd, mctx->cq);
	if (!mctx->qp || IS_ERR(mctx->qp)) {
		ret = -ENODEV;
		goto destroy_mcast_cq;
	}

	init_completion(&ctrl->mcast_done);
	str_to_ib_sockaddr("::", &ctrl->mcast_addr);
	ret = rdma_join_multicast(mctx->cm_id, (struct sockaddr *)
			&ctrl->mcast_addr, BIT(FULLMEMBER_JOIN), mctx);
	if (ret) {
		pr_err("rdma_join_multicast failed: %d\n", ret);
		goto destroy_mcast_qp;
	}

	ret = mcswap_wait_for_mcast_join();
	if (ret && !ctrl->cm_error) {
		goto destroy_mcast_ah;
	} else if (ret && ctrl->cm_error) {
		goto leave_mcast_grp;
	}

	return 0;

destroy_mcast_ah:
#if LINUX_VERSION_CODE >= KERNEL_VERSION(5, 0, 0)
	rdma_destroy_ah(mctx->ah, RDMA_DESTROY_AH_SLEEPABLE);
#elif LINUX_VERSION_CODE >= KERNEL_VERSION(4, 15, 0)
	rdma_destroy_ah(mctx->ah);
#else
	ib_destroy_ah(mctx->ah);
#endif
leave_mcast_grp:
	rdma_leave_multicast(mctx->cm_id,
			(struct sockaddr *) &ctrl->mcast_addr);
destroy_mcast_qp:
	rdma_destroy_qp(mctx->cm_id);
destroy_mcast_cq:
	ib_destroy_cq(mctx->cq);
destroy_mcast_pd:
	ib_dealloc_pd(ctrl->pd);
destroy_mcast_cm_id:
	rdma_destroy_id(mctx->cm_id);
	return ret;
}

static int __init mcswap_alloc_ctrl_resources(void) {
	ctrl = kzalloc(sizeof *ctrl, GFP_KERNEL);
	if (!ctrl) {
		pr_err("failed to alloc memory for ctrl structure\n");
		goto exit;
	}
	atomic_set(&ctrl->inflight_loads, 0);

	ctrl->server = kzalloc(sizeof *ctrl->server * nr_servers, GFP_KERNEL);
	if (!ctrl->server) {
		pr_err("failed to alloc memory for server ctrl structure\n");
		goto free_ctrl;
	}

	mcswap_entry_cache = KMEM_CACHE(mcswap_entry, 0);
	if (unlikely(!mcswap_entry_cache)) {
		pr_err("failed to create slab cache for mcswap entries\n");
		goto free_ctx;
	}

	page_ack_req_cache = KMEM_CACHE(page_ack_req, 0);
	if (unlikely(!page_ack_req_cache)) {
		pr_err("failed to create slab cache for page ack req entries\n");
		goto destroy_ent_cache;
	}

	rdma_req_cache = KMEM_CACHE(rdma_req, 0);
	if (unlikely(!rdma_req_cache)) {
		pr_err("failed to create slab cache for rdma req entries\n");
		goto destroy_ack_cache;
	}
	return 0;

free_ctx:
	kfree(ctrl->server);
free_ctrl:
	kfree(ctrl);
destroy_ent_cache:
	kmem_cache_destroy(mcswap_entry_cache);
destroy_ack_cache:
	kmem_cache_destroy(page_ack_req_cache);
exit:
	return -ENOMEM;
}

static void mcswap_destroy_mcast_resources(void) {

}

static void mcswap_destroy_caches(void) {
	kmem_cache_destroy(mcswap_entry_cache);
	kmem_cache_destroy(page_ack_req_cache);
	kmem_cache_destroy(rdma_req_cache);
}


static int __init mcswap_server_connect( struct server_ctx *ctx) {
	int ret;
	pr_info("%s(ip=%s, port=%d)\n", __func__, ctx->ip, ctx->port);
	ctx->cm_id = rdma_create_id(&init_net, mcswap_cm_event_handler,
			ctx, RDMA_PS_TCP, IB_QPT_RC);
	if (IS_ERR(ctx->cm_id)) {
		pr_err("failed to create CM ID: %ld\n", PTR_ERR(ctx->cm_id));
		return -ENODEV;
	}

	ret = mcswap_parse_ip_addr(&ctx->sin_addr, ctx->ip);
	if (ret) {
		pr_err("failed to parse server ip addr %s (code = %d)\n", ctx->ip, ret);
		goto destroy_cm_id;
	}
	ctx->sin_addr.sin_port = cpu_to_be16(ctx->port);

	// Resolve destination and optionally source addresses from IP addresses to an
	// RDMA address and if the resolution is successful, then the RDMA cm id will
	// be bound to an RDMA device.
	init_completion(&ctx->cm_done);
	ret = rdma_resolve_addr(ctx->cm_id, (struct sockaddr *)
			&ctrl->client_addr, (struct sockaddr *) &ctx->sin_addr,
			MCSWAP_RDMA_CONNECTION_TIMEOUT_MS);
	if (ret) {
		pr_err("rdma_resolve_addr failed: %d\n", ret);
		goto destroy_cm_id;
	}

	// block until the reliable connection to the remote memory server is
	// established or until connection establishment times out
	ret = mcswap_wait_for_rc_establishment(ctx);
	if (ret) {
		goto destroy_cm_id;
	}

	// post a recv buffer for the incoming message from the memory server
	// that will ACK/NACK multicast membership
	init_completion(&ctx->mcast_ack);
	ret = mcswap_post_recv_control_msg(ctx);
	if (ret) {
		goto destroy_mcast_resources;
	}

	// block until the remote memory server joins the multicast group or
	// until the timeout to join expires
	ret = mcswap_wait_for_server_mcast_ack(ctx);
	if (ret) {
		goto destroy_mcast_resources;
	}
	return 0;

destroy_mcast_resources:
	// TODO(dimlek): destroy resources here
destroy_cm_id:
	rdma_destroy_id(ctx->cm_id);
	return ret;
}

static int __init mcswap_parse_module_params(void) {
	char *endpoint_dup[MAX_NR_SERVERS];
	const char *delim = ":";
	int i, ret;
	ctrl->n_servers = nr_servers;
	if (ctrl->n_servers > MAX_NR_SERVERS) {
		pr_warn("max memory servers allowed: %d\n", MAX_NR_SERVERS);
		return -ENOTSUPP;
	}

	ret = mcswap_parse_ip_addr(&ctrl->client_addr, client_ip);
	if (ret) {
		pr_err("failed to parse client_ip addr %s (code = %d)\n",
				client_ip, ret);
		return ret;
	}

	for (i = 0; i < ctrl->n_servers; i++) {
		// duplicate endpoint info because it will be modified by strsep
		endpoint_dup[i] = kstrdup(endpoint[i], GFP_KERNEL);

		// obtain ip address from endpoint
		ctrl->server[i].ip = strsep(&endpoint_dup[i], delim);
		if (!endpoint_dup[i]) {
			pr_err("failed to parse server endpoint %s\n", endpoint[i]);
			kfree(endpoint_dup[i]);
			return -EINVAL;
		}

		// obtain port number from endpoint
		ret = kstrtoint(strsep(&endpoint_dup[i], delim), 0,
				&ctrl->server[i].port);
		if (ret) {
			pr_err("failed to parse server endpoint %s\n", endpoint[i]);
			kfree(endpoint_dup[i]);
			return ret;
		}
		ctrl->server[i].sidx = i;
	}

	return 0;
}

static int __init mcswap_init(void) {
	enum ib_poll_context poll_ctx;
	int comp_vector = 0;
	int ret, i;
	ret = mcswap_alloc_ctrl_resources();
	if (ret) {
		return ret;
	}

	ret = mcswap_parse_module_params();
	if (ret) {
		return ret;
	}

	ret = mcswap_rdma_mcast_init(&ctrl->mcast);
	if (ret) {
		pr_err("failed to setup multicast\n");
		goto destroy_caches;
	}

	if (!ctrl->dev) {
		return -ENODEV;
	}

	// since we already have a handle here on the HCA, we can create the
	// shared send completion queue for all the RCs to the mem servers
	poll_ctx = enable_poll_mode ? IB_POLL_DIRECT : IB_POLL_SOFTIRQ;
	ctrl->send_cq = ib_alloc_cq(ctrl->dev, ctrl, 8096,
			comp_vector, poll_ctx);
	if (IS_ERR(ctrl->send_cq)) {
		ret = PTR_ERR(ctrl->send_cq);
		goto destroy_mcast_resources;
	}

	for (i = 0; i < ctrl->n_servers; i++) {
		ret = mcswap_server_connect(&ctrl->server[i]);
		if (ret)
			goto destroy_send_cq;
	}

	if (enable_async_mode) {
		frontswap_register_ops(&mcswap_async_ops);
		frontswap_writethrough(true);
	} else {
		frontswap_register_ops(&mcswap_sync_ops);
	}

	if (mcswap_debugfs_init()) {
		pr_warn("debugfs initialization failed.\n");
	}

	pr_info("module loaded successfully.\n");
	return 0;

destroy_send_cq:
	ib_free_cq(ctrl->send_cq);
destroy_mcast_resources:
	mcswap_destroy_mcast_resources();
destroy_caches:
	mcswap_destroy_caches();
	return ret;
}

static void __exit mcswap_exit(void) {
	mcswap_destroy_caches();
	mcswap_destroy_mcast_resources();
}

module_init(mcswap_init);
module_exit(mcswap_exit);

MODULE_AUTHOR("Dimitris Lekkas, dlekkasp@gmail.com");
MODULE_DESCRIPTION("Low latency and memory efficient paging over RDMA");
MODULE_LICENSE("GPL v2");
