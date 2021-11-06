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

#include <rdma/ib_verbs.h>
#include <rdma/rdma_cm.h>
#include <rdma/ib_mad.h>
#include <rdma/ib.h>

#include <linux/init.h>
#include <linux/hashtable.h>
#include <linux/rbtree.h>

#define MCSWAP_RDMA_CONNECTION_TIMEOUT_MS 3000
#define MCSWAP_MCAST_JOIN_TIMEOUT_MS 5000
#define MCSWAP_MAX_SEND_RETRIES 3
#define MCSWAP_PAGE_ACK_TIMEOUT_MS 1
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
	pgoff_t offset;
	struct page_info info;
};

struct mcswap_tree {
	struct rb_root root;
	spinlock_t lock;
};

static struct mcswap_tree *mcswap_trees[MAX_SWAPFILES];

struct rdma_req {
	struct ib_cqe cqe;
	struct completion done;
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
			return -1;
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
	return -1;
}

struct mem_info {
	u64 addr;
	u32 len;
	u32 key;
};

struct ud_transport_info {
	// The GID that is used to identify the destination port of the packets
	union ib_gid gid;
	// If destination is on same subnet, the LID of the port to which the
	// subnet delivers the packets to
	u32 lid;
	// The QP number of the remote UC QP
	u32 qpn;
	// use ib_find_pkey() to find the index of a given pkey
	u32 qkey;
	// default IB_DEFAULT_PKEY_FULL -> defined in <rdma/ib_mad.h>
	u32 pkey;
};

struct server_info {
	struct mem_info mem;
	struct ud_transport_info ud_transport;
};

struct server_ctx {
	struct rdma_cm_id *id;
	struct ib_qp *qp;
	struct ib_pd *pd;

	struct ib_cq *send_cq;
	struct ib_cq *recv_cq;

	struct mem_info mem;
	struct sockaddr_in sin_addr;

	struct completion cm_done;
	int cm_error;
};

struct swapmon_rdma_ctrl {
	struct ib_device *dev;

	// TODO(dimlek): maybe use the same PD for both (?)
	struct ib_pd *pd;
	struct ib_pd *mcast_pd;


	struct ib_qp *mcast_qp;

	struct ib_cq *mcast_cq;

	//struct server_info server;
	struct server_ctx *server;

	struct ib_ah *ah;

	struct ib_ah *mcast_ah;

	u32 remote_mcast_qpn;
	u32 remote_mcast_qkey;

	spinlock_t cq_lock;

	enum ib_qp_type qp_type;

	// RDMA identifier created through `rdma_create_id`
	struct rdma_cm_id *cm_id;
	struct rdma_cm_id *mcast_cm_id;

	struct completion mcast_done;

	struct completion cm_done;
	atomic_t pending;
	int cm_error;

	struct completion mcast_ack;

	// IPv4 addresses
	struct sockaddr_in client_addr;

	// Infiniband address
	struct sockaddr_ib mcast_addr;

	int n_servers;
};

struct swapmon_transport_ops {
	struct swapmon_rdma_ctrl *(*create_ctrl)(struct device *dev);
};

struct swapmon_rdma_ctrl* ctrl;


static void swapmon_init(unsigned swap_type) {
	struct mcswap_tree *tree;
	pr_info("%s(%d)", __func__, swap_type);

	tree = kzalloc(sizeof(*tree), GFP_KERNEL);
	if (!tree) {
		pr_err("failed to alloc memory for red-black tree.\n");
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

static int swapmon_mcast_send(struct page *page, pgoff_t offset) {
	struct ib_send_wr *bad_wr;
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
	sge.lkey = ctrl->mcast_pd->local_dma_lkey;

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

	ud_wr.ah = ctrl->mcast_ah;
	ud_wr.remote_qpn = ctrl->remote_mcast_qpn;
	ud_wr.remote_qkey = ctrl->remote_mcast_qkey;

	ret = ib_post_send(ctrl->mcast_qp, &ud_wr.wr, &bad_wr);
	if (ret) {
		pr_err("ib_post_send failed to send page (error = %d).\n", ret);
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
	tree = mcswap_trees[req->swap_type];
	ent = req->entry;

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
		pr_info("[%u] page %lu stored at: remote addr = %llu, rkey = %u\n",
				atomic_read(mcswap_stored_pages), offset, ent->info.remote_addr,
				ent->info.rkey);
  }
#else
	spin_unlock_bh(&tree->lock);
#endif
	kmem_cache_free(page_ack_req_cache, req);
}


static int mcswap_post_recv_page_ack(struct page_ack_req *req) {
	struct ib_recv_wr *bad_wr;
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
			pr_err("ib_post_recv failed to post RR"
					"for page ack (code = %d)\n", ret);
			return ret;
		}
	}

	return 0;
}


/*
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
		ret = post_recv_page_ack_msg(req);
		if (ret) {
			pr_err("post_recv_page_ack_msg failed.\n");
			goto free_page_req;
		}

		// multicast the physical page to the memory servers
		ret = swapmon_mcast_send(page, offset);
		if (unlikely(ret)) {
			pr_err("swapmon_mcast_send() failed.\n");
			goto free_page_req;
		}
		mcswap_sent_pages++;

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
*/

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
	//ret = swapmon_store_sync(swap_type, offset, page);
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
	struct mcswap_entry* rmem_node;
	struct mcswap_entry* ent;
	struct page_ack_req* req;
	int i, ret;

	ret = kmem_cache_alloc_bulk(mcswap_entry_cache, GFP_KERNEL,
			ctrl->n_servers, (void *) &ent);
	if (unlikely(ret)) {
		pr_err("kmem_cache_alloc_bulk failed: %d\n", ret);
		return -ENOMEM;
	}

	ret = kmem_cache_alloc_bulk(page_ack_req_cache, GFP_KERNEL,
			ctrl->n_servers, (void *) &req);
	if (unlikely(ret)) {
		pr_err("kmem_cache_alloc_bulk failed: %d\n", ret);
		return -ENOMEM;
	}

	for (i = 0; i < ctrl->n_servers; i++) {
		ent[i].offset = offset;
		req[i].entry = rmem_node;
		req[i].cqe.done = page_ack_done;
		req[i].swap_type = swap_type;
	}

	// post RR for the ACK message to be received from mem server
	ret = mcswap_post_recv_page_ack(req);
	if (ret) {
		pr_err("mcswap_post_recv_page_ack failed\n");
		return ret;
	}

	// multicast the physical page to the memory servers
	ret = swapmon_mcast_send(page, offset);
	if (unlikely(ret)) {
		pr_err("swapmon_mcast_send() failed.\n");
		return ret;
	}
	/*
	pr_info("[%u] %s(swap_type = %u, offset = %lu)\n",
				 atomic_read(&mcswap_stored_pages), __func__,
				 swap_type, offset);
				 */
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
	complete(&req->done);

	ib_dma_unmap_page(ctrl->dev, req->dma, PAGE_SIZE, DMA_BIDIRECTIONAL);
}

static int swapmon_rdma_read_sync(struct page *page,
		struct mcswap_entry *ent) {
	struct ib_send_wr *bad_send_wr;
	struct ib_rdma_wr rdma_wr;
	struct rdma_req *req;
	struct ib_sge sge;
	u64 dma_addr;
	long wait_ret;
	int ret;

  dma_addr = ib_dma_map_page(ctrl->dev, page, 0,
			PAGE_SIZE, DMA_BIDIRECTIONAL);
	if (unlikely(ib_dma_mapping_error(ctrl->dev, dma_addr))) {
		pr_err("%s: dma mapping error\n", __func__);
		return -1;
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
	req->cqe.done = page_read_done;
	req->dma = dma_addr;
	init_completion(&req->done);

	rdma_wr.wr.wr_cqe = &req->cqe;
	rdma_wr.wr.next = NULL;
	rdma_wr.wr.sg_list = &sge;
	rdma_wr.wr.num_sge = 1;
	rdma_wr.wr.opcode = IB_WR_RDMA_READ;
	rdma_wr.wr.send_flags = IB_SEND_SIGNALED;

	rdma_wr.remote_addr = ent->info.remote_addr;
	rdma_wr.rkey = ent->info.rkey;

	// TODO(dimlek): fix it below
	ret = ib_post_send(ctrl->server[0].qp, &rdma_wr.wr, &bad_send_wr);
	if (unlikely(ret)) {
		pr_err("ib_post_send failed to read remote page.\n");
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

	return 0;
}



static int swapmon_load(unsigned swap_type, pgoff_t offset,
		struct page *page) {
	struct mcswap_tree *tree = mcswap_trees[swap_type];
	struct mcswap_entry *entry;
	int ret;

	spin_lock_bh(&tree->lock);
	entry = rb_find_page_remote_info(&tree->root, offset);
	if (!entry) {
		pr_err("failed to find page metadata in red-black tree.\n");
		spin_unlock_bh(&tree->lock);
		return -1;
	}
	spin_unlock_bh(&tree->lock);

	ret = swapmon_rdma_read_sync(page, entry);
	if (unlikely(ret)) {
		pr_info("failed to fetch remote page.\n");
		return -1;
	}

	return 0;
}

static int __mcswap_load_sync(unsigned swap_type, pgoff_t offset,
		struct page *page) {
	int ret;
#ifdef BENCH
	struct timespec ts_start, ts_end;
	int n_pages = atomic_read(&mcswap_loaded_pages);
	long elapsed;
#endif
#ifdef DEBUG
	struct mcswap_tree *tree = mcswap_trees[swap_type];
	struct mcswap_entry *ent;
	pr_info("[%u] %s(swap_type = %u, offset = %lu)\n",
				 atomic_read(&mcswap_loaded_pages), __func__,
				 swap_type, offset);
#endif
#ifdef BENCH
	getnstimeofday(&ts_start);
#endif
	ret = swapmon_load(swap_type, offset, page);
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
#ifdef DEBUG
	pr_info("[%u] page %lu loaded from: remote addr = %llu, rkey = %u\n",
			atomic_read(&mcswap_loaded_pages), offset, entry->info.remote_addr,
			entry->info.rkey);
#endif
	return 0;
}


static void swapmon_invalidate_page(unsigned swap_type, pgoff_t offset) {
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

static void swapmon_invalidate_area(unsigned swap_type) {
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

static struct frontswap_ops mcswap_sync_ops = {
	// prepares the device to receive frontswap pages associated with the
	// specified swap device number (aka "type"). This function is invoked
	// when we `swapon` a device.
	.init  = swapmon_init,
	// copies the page to transcendent memory and associate it with the type
	// and offset associated with the page.
	.store = __mcswap_store_sync,
	// copies the page, if found, from transcendent memory into kernel memory,
	// but will not remove the page from transcendent memory.
	.load  = __mcswap_load_sync,
	// removes the page from transcendent memory.
	.invalidate_page = swapmon_invalidate_page,
	// removes all pages from transcendent memory.
	.invalidate_area = swapmon_invalidate_area,
};

static struct frontswap_ops mcswap_async_ops = {
	.init = swapmon_init,
	.store = mcswap_store_async,
	.load = __mcswap_load_sync,
	.invalidate_page = swapmon_invalidate_page,
	.invalidate_area = swapmon_invalidate_area
};


/*--------------------- Module parameters ---------------------*/

#define MAX_NR_SERVERS 3

static char *server_ip[MAX_NR_SERVERS];
static int nr_servers;
module_param_array(server_ip, charp, &nr_servers, 0644);
MODULE_PARM_DESC(server_ip, "IP addresses of memory servers in n.n.n.n form");

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

/*-------------------------------------------------------------*/


static int swapmon_rdma_parse_ipaddr(struct sockaddr_in *saddr, char *ip) {
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
	init_attr.send_cq = ctx->send_cq;
	init_attr.recv_cq = ctx->recv_cq;

	// allocate a queue pair (QP) and associate it with the specified
	// RDMA identifier cm_id
	ret = rdma_create_qp(ctx->id, ctx->pd, &init_attr);
	if (ret) {
		pr_err("rdma_create_qp failed: %d\n", ret);
		return ret;
	}
	ctx->qp = ctx->id->qp;
	return ret;
}

static int mcswap_rdma_addr_resolved(struct rdma_cm_id *cm_id) {
	struct server_ctx *ctx = cm_id->context;
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

	// check whether memory registrations are supported
	if (!(ctrl->dev->attrs.device_cap_flags &
				IB_DEVICE_MEM_MGT_EXTENSIONS)) {
		pr_err("Memory registrations not supported.\n");
		ret = -ENOTSUPP;
		goto out_err;
	}

	// Before creating the local QP, we must first cause the HCA to create the CQs
	// to be associated with the SQ and RQ of the QP about to be created. The QP's
	// SQ and RQ can have separate CQs or may share a CQ. The 3rd argument
	// specifies the number of Completion Queue Entries (CQEs) and represents the
	// size of the CQ.
	ctx->send_cq = ib_alloc_cq(ctrl->dev, ctx, 8096,
			comp_vector, IB_POLL_SOFTIRQ);
	if (IS_ERR(ctx->send_cq)) {
		ret = PTR_ERR(ctx->send_cq);
		goto out_err;
	}

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
	ret = rdma_resolve_route(ctx->id,
			MCSWAP_RDMA_CONNECTION_TIMEOUT_MS);
	if (ret) {
		pr_err("rdma_resolve_route failed: %d\n", ret);
		goto out_destroy_qp;
	}
	return 0;

out_destroy_qp:
	rdma_destroy_qp(ctx->id);
out_destroy_recv_cq:
	ib_free_cq(ctx->send_cq);
out_destroy_send_cq:
	ib_free_cq(ctx->recv_cq);
out_err:
	return ret;
}


static int mcswap_rdma_route_resolved(struct server_ctx *ctx) {
	struct rdma_conn_param param = {};
  int ret;

  param.private_data = NULL;
  param.private_data_len = 0;
  param.responder_resources = 8;
  param.initiator_depth = 8;
  param.flow_control = 0;
  param.retry_count = MCSWAP_MAX_SEND_RETRIES;
  param.rnr_retry_count = MCSWAP_MAX_SEND_RETRIES;
  param.qp_num = ctx->qp->qp_num;

  pr_info("max_qp_rd_atom=%d max_qp_init_rd_atom=%d\n",
      ctrl->dev->attrs.max_qp_rd_atom,
      ctrl->dev->attrs.max_qp_init_rd_atom);

	// Initiate an active connection request (i.e. send REQ message). The route
	// must have been resolved before trying to connect.
  ret = rdma_connect(ctx->id, &param);
  if (ret) {
    pr_err("rdma_connect failed: %d\n", ret);
    ib_free_cq(ctx->send_cq);
    ib_free_cq(ctx->recv_cq);
		return ret;
  }
	pr_info("trying to rdma_connect() ...\n");

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


static int mcswap_mcast_join_handler(struct rdma_cm_event *event) {
	struct rdma_ud_param *param = &event->param.ud;
	if (param->ah_attr.type != RDMA_AH_ATTR_TYPE_IB) {
		pr_err("only IB address handle type is supported\n");
		return -ENOTSUPP;
	}

	pr_info("UD Multicast membership info: dgid = %16phC, mlid = 0x%x, "
			"sl = %d, src_path_bits = %u, qpn = %u, qkey = %u\n",
			param->ah_attr.grh.dgid.raw, param->ah_attr.ib.dlid,
			rdma_ah_get_sl(&param->ah_attr), rdma_ah_get_path_bits(&param->ah_attr),
			param->qp_num, param->qkey);

	ctrl->remote_mcast_qpn = param->qp_num;
	ctrl->remote_mcast_qkey = param->qkey;
	ctrl->mcast_ah = rdma_create_ah(ctrl->pd, &param->ah_attr);
	if (!ctrl->mcast_ah) {
		pr_err("failed to create address handle for multicast\n");
		return -ENOENT;
	}
	return 0;
}

static int recv_mem_info(struct ib_qp *qp) {
	struct ib_recv_wr *bad_wr;
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

	// Map kernel virtual address `ctrl->rmem` to a DMA address.
	dma_addr = ib_dma_map_single(ctrl->dev, msg,
			sizeof(struct server_info), DMA_FROM_DEVICE);
	if (ib_dma_mapping_error(ctrl->dev, dma_addr)) {
		pr_err("dma address mapping error.\n");
	}

	// Prepare DMA region to be accessed by device.
	ib_dma_sync_single_for_device(ctrl->dev, dma_addr,
			sizeof(struct server_info), DMA_FROM_DEVICE);

	sge.addr = dma_addr;
	sge.length = sizeof(struct server_info);
	sge.lkey = ctrl->pd->local_dma_lkey;

	wr.next = NULL;
	wr.wr_id = 0;
	wr.sg_list = &sge;
	wr.num_sge = 1;

	ret = ib_post_recv(qp, &wr, &bad_wr);
	if (ret) {
		pr_err("ib_post_recv failed to receive remote mem info.\n");
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

	/*
	ctrl->server.mem.addr = msg->addr;
	ctrl->server.mem.len  = msg->len;
	ctrl->server.mem.key  = msg->key;
	*/
	pr_info("Remote mem info: addr = %llu, len = %u, key = %u\n",
			msg->addr, msg->len, msg->key);

	return 0;
}

static int mcswap_cm_event_handler(struct rdma_cm_id *id,
		struct rdma_cm_event *event) {
	int cm_error = 0;
	int ret;
	printk("%s msg: %s (%d) status %d id $%p\n", __func__,
				 rdma_event_msg(event->event), event->event, event->status, id);

	switch (event->event) {
		case RDMA_CM_EVENT_ADDR_RESOLVED:
			cm_error = mcswap_rdma_addr_resolved(id);
			break;
		case RDMA_CM_EVENT_ROUTE_RESOLVED:
			cm_error = mcswap_rdma_route_resolved(id->context);
			break;
		case RDMA_CM_EVENT_ESTABLISHED:
			pr_info("connection established successfully\n");
			// Wait to receive memory info to retrieve all required info to properly
			// reference remote memory.
			ret = recv_mem_info(id->qp);
			if (ret) {
				pr_err("unable to receive remote mem info.\n");
			}
			break;
		case RDMA_CM_EVENT_ROUTE_ERROR:
		case RDMA_CM_EVENT_ADDR_ERROR:
		case RDMA_CM_EVENT_CONNECT_REQUEST:
		case RDMA_CM_EVENT_CONNECT_RESPONSE:
			pr_info("successful response to the connection request (REQ).\n");
			break;
		case RDMA_CM_EVENT_CONNECT_ERROR:
			pr_err("error while trying to establish connection.\n");
			break;
		case RDMA_CM_EVENT_UNREACHABLE:
			pr_info("remote server is unreachable or unable to respond to \
					connection request.\n");
			break;
		case RDMA_CM_EVENT_REJECTED:
			pr_info("Connection request or response rejected by remote endpoint.\n");
			break;
		case RDMA_CM_EVENT_DISCONNECTED:
			pr_info("RDMA connection was closed.\n");
			break;
		case RDMA_CM_EVENT_DEVICE_REMOVAL:
			break;
		case RDMA_CM_EVENT_MULTICAST_JOIN:
			pr_info("multicast join operation completed successfully\n");
			// post_recv_control_msg(1);
			cm_error = mcswap_mcast_join_handler(event);
			ctrl->cm_error = cm_error;
			complete(&ctrl->mcast_done);
			break;
		case RDMA_CM_EVENT_MULTICAST_ERROR:
			break;
		case RDMA_CM_EVENT_ADDR_CHANGE:
		case RDMA_CM_EVENT_TIMEWAIT_EXIT:
			break;
		default:
			pr_err("received unrecognized RDMA CM event %d\n", event->event);
			break;
	}

	if (cm_error) {
		pr_info("cm_error=%d\n", cm_error);
		ctrl->cm_error = cm_error;
		// complete(&ctrl->cm_done);
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

static int __init mcswap_wait_for_mcast_join(void) {
	int wait_ret = wait_for_completion_interruptible_timeout(
			&ctrl->mcast_done, msecs_to_jiffies(MCSWAP_MCAST_JOIN_TIMEOUT_MS) + 1);
	if (wait_ret == 0) {
		pr_err("timed out on multicast join waiting\n");
		return -ETIME;
	} else if (wait_ret == -ERESTARTSYS) {
		pr_err("interrupted on multicast join waiting\n");
		return wait_ret;
	}
	return 0;
}

static int __init mcswap_rdma_bind_addr(struct rdma_cm_id *id,
		struct sockaddr_in *sin, char *ip) {
	int ret = swapmon_rdma_parse_ipaddr(sin, ip);
	if (ret) {
		pr_err("failed to parse client ip addr %s (code = %d)\n",
				client_ip, ret);
		return ret;
	}

	ret = rdma_bind_addr(id, (struct sockaddr *) sin);
	if (ret) {
		pr_err("rdma_bind_addr failed: %d\n", ret);
		return ret;
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

static int __init mcswap_rdma_mcast_init(void) {
	int ret;

	ctrl->mcast_cm_id = rdma_create_id(&init_net, mcswap_cm_event_handler,
			ctrl, RDMA_PS_UDP, IB_QPT_UD);
	if (IS_ERR(ctrl->mcast_cm_id)) {
		pr_err("multicast cm id creation failed\n");
		return -ENODEV;
	}

	ret = mcswap_rdma_bind_addr(ctrl->mcast_cm_id,
			&ctrl->client_addr, client_ip);
	if (ret) {
		goto destroy_mcast_cm_id;
	}

	// since a specific local address was given (i.e. `client_addr`) to
	// the `rdma_bind_addr` call then the RDMA identifier will be bound
	// to the local RDMA device, so we can get a handle to the HCA
	ctrl->dev = ctrl->mcast_cm_id->device;
	ret = mcswap_check_hca_support(ctrl->dev, ctrl->mcast_cm_id->port_num);
	if (ret) {
		goto destroy_mcast_cm_id;
	}

	ctrl->pd = ib_alloc_pd(ctrl->dev, 0);
	if (IS_ERR(ctrl->pd)) {
		pr_err("failed to alloc protection domain\n");
		ret = PTR_ERR(ctrl->pd);
		goto destroy_mcast_cm_id;
	}

	ctrl->mcast_cq = ib_alloc_cq(ctrl->dev, ctrl,
			MCSWAP_MAX_MCAST_CQE, 0, IB_POLL_SOFTIRQ);
	if (IS_ERR(ctrl->mcast_cq)) {
		pr_err("failed to alloc completion queue\n");
		ret = PTR_ERR(ctrl->mcast_cq);
		goto destroy_mcast_pd;
	}

	ctrl->mcast_qp = mcswap_create_mcast_qp(ctrl->mcast_cm_id,
			ctrl->pd, ctrl->mcast_cq);
	if (!ctrl->mcast_qp || IS_ERR(ctrl->mcast_qp)) {
		ret = -ENODEV;
		goto destroy_mcast_cq;
	}

	init_completion(&ctrl->mcast_done);
	str_to_ib_sockaddr("::", &ctrl->mcast_addr);
	ret = rdma_join_multicast(ctrl->mcast_cm_id, (struct sockaddr *)
			&ctrl->mcast_addr, BIT(FULLMEMBER_JOIN), ctrl);
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
	rdma_destroy_ah(ctrl->mcast_ah);
leave_mcast_grp:
	rdma_leave_multicast(ctrl->mcast_cm_id,
			(struct sockaddr *) &ctrl->mcast_addr);
destroy_mcast_qp:
	rdma_destroy_qp(ctrl->mcast_cm_id);
destroy_mcast_cq:
	ib_destroy_cq(ctrl->mcast_cq);
destroy_mcast_pd:
	ib_dealloc_pd(ctrl->pd);
destroy_mcast_cm_id:
	rdma_destroy_id(ctrl->mcast_cm_id);
	return ret;
}

static int __init mcswap_alloc_ctrl_resources(void) {
	ctrl = kzalloc(sizeof(struct swapmon_rdma_ctrl), GFP_KERNEL);
	if (!ctrl) {
		pr_err("failed to alloc memory for ctrl structure\n");
		goto exit;
	}

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

static void __exit mcswap_destroy_caches(void) {
	kmem_cache_destroy(mcswap_entry_cache);
	kmem_cache_destroy(page_ack_req_cache);
	kmem_cache_destroy(rdma_req_cache);
}


static int __init mcswap_server_connect(
		struct server_ctx *ctx, char *ip, int port) {
	int ret;
	pr_info("%s(ip=%s, port=%d)\n", __func__, ip, port);
	ctx->id = rdma_create_id(&init_net, mcswap_cm_event_handler,
			ctx, RDMA_PS_TCP, IB_QPT_RC);
	if (IS_ERR(ctx->id)) {
		pr_err("failed to create CM ID: %ld\n", PTR_ERR(ctx->id));
		return -ENODEV;
	}

	ret = swapmon_rdma_parse_ipaddr(&ctx->sin_addr, ip);
	if (ret) {
		pr_err("failed to parse server ip addr %s (code = %d)\n", ip, ret);
		goto destroy_cm_id;
	}
	ctx->sin_addr.sin_port = cpu_to_be16(port);

	ret = swapmon_rdma_parse_ipaddr(&ctrl->client_addr, client_ip);
	if (ret) {
		pr_err("failed to parse client ip addr %s (code = %d)\n", client_ip, ret);
		goto destroy_cm_id;
	}

	// Resolve destination and optionally source addresses from IP addresses to an
	// RDMA address and if the resolution is successful, then the RDMA cm id will
	// be bound to an RDMA device.
	ret = rdma_resolve_addr(ctx->id, (struct sockaddr *)
			&ctrl->client_addr, (struct sockaddr *) &ctx->sin_addr,
			MCSWAP_RDMA_CONNECTION_TIMEOUT_MS);
	if (ret) {
		pr_err("rdma_resolve_addr failed: %d\n", ret);
		goto destroy_cm_id;
	}
	return 0;

destroy_cm_id:
	rdma_destroy_id(ctx->id);
	return ret;
}

static int __init mcswap_init(void) {
	int ret, i;
	ret = mcswap_alloc_ctrl_resources();
	if (ret) {
		return ret;
	}

	if (nr_servers > MAX_NR_SERVERS) {
		pr_warn("max supported memory servers: %d\n", MAX_NR_SERVERS);
		nr_servers = MAX_NR_SERVERS;
	}
	ctrl->n_servers = nr_servers;

	ret = mcswap_rdma_mcast_init();
	if (ret) {
		pr_err("failed to setup multicast\n");
		goto destroy_caches;
	}

	for (i = 0; i < ctrl->n_servers; i++) {
		ret = mcswap_server_connect(&ctrl->server[i],
				server_ip[i], server_port);
		if (ret)
			goto destroy_mcast_resources;
		pr_info("connected successfully to server %s\n", server_ip[i]);
	}

	return -1;

	if (!enable_async_mode) {
		frontswap_register_ops(&mcswap_sync_ops);
	} else {
		frontswap_register_ops(&mcswap_async_ops);
		frontswap_writethrough(true);
	}

	if (mcswap_debugfs_init()) {
		pr_warn("debugfs initialization failed.\n");
	}

	pr_info("module loaded successfully.\n");
	return 0;

destroy_mcast_resources:
	mcswap_destroy_mcast_resources();
destroy_caches:
	mcswap_destroy_caches();
	return ret;
}

static void __exit mcswap_exit(void) {
	mcswap_destroy_caches();
	pr_info("module unloaded\n");
}

module_init(mcswap_init);
module_exit(mcswap_exit);

MODULE_AUTHOR("Dimitris Lekkas, dlekkasp@gmail.com");
MODULE_DESCRIPTION("Low latency and memory efficient paging over RDMA");
MODULE_LICENSE("GPL v2");
