/*
 * swapmon.c - swapping over RDMA driver
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

#define SWAPMON_RDMA_CONNECTION_TIMEOUT_MS 3000
#define SWAPMON_MCAST_JOIN_TIMEOUT_MS 5000
#define UC_QKEY 0x11111111

int page_cnt = 0;

static int swapmon_rdma_cm_event_handler(struct rdma_cm_id *id,
		struct rdma_cm_event *event);

static struct kmem_cache *mcswap_entry_cache;
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

struct page_node {
	struct page_info info;
	pgoff_t offset;
	struct hlist_node node;
};

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

struct swapmon_rdma_ctrl {
	struct ib_device *dev;

	// TODO(dimlek): maybe use the same PD for both (?)
	struct ib_pd *pd;
	struct ib_pd *mcast_pd;


	// queue pair (QP)
	struct ib_qp *qp;
	struct ib_qp *ud_qp;
	struct ib_qp *mcast_qp;

	// completion queue (CQ)
	struct ib_cq *send_cq;
	struct ib_cq *recv_cq;
	struct ib_cq *mcast_cq;

	struct server_info server;

	struct ib_ah *ah;

	struct ib_ah *mcast_ah;

	u32 remote_mcast_qpn;
	u32 remote_mcast_qkey;

	spinlock_t cq_lock;

	enum ib_qp_type qp_type;

	// RDMA identifier created through `rdma_create_id`
	struct rdma_cm_id *cm_id;
	struct rdma_cm_id *mcast_cm_id;

	struct completion cm_done;
	atomic_t pending;
	int cm_error;

	struct completion mcast_ack;

	int queue_size;

	// RDMA connection state
	enum rdma_cm_event_type state;

	// IPv4 addresses
	struct sockaddr_in server_addr;
	struct sockaddr_in client_addr;

	// Infiniband address
	struct sockaddr_ib mcast_addr;
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
	//ud_wr.wr.wr_id = 1111;
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
	struct mcswap_entry *ent;
	struct page_ack_req *req;
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
	spin_lock(&tree->lock);
	ret = mcswap_rb_insert(&tree->root, req->entry);
	if (ret) {
		// entry already exists for this offset, so we need to release
		// the lock and free the mcswap_entry structure here.
		spin_unlock(&tree->lock);
		kmem_cache_free(mcswap_entry_cache, req->entry);
	} else {
		// entry was added to the remote memory mapping
		ent = rb_find_page_remote_info(&tree->root, offset);
		spin_unlock(&tree->lock);
		pr_info("[%u] page %lu stored at: remote addr = %llu, rkey = %u\n",
				page_cnt, offset, ent->info.remote_addr, ent->info.rkey);
  }
	kmem_cache_free(rdma_req_cache, req);
}

static int post_recv_page_ack_msg(struct page_ack_req *req) {
	struct ib_recv_wr wr, *bad_wr;
	struct mcswap_entry *entry;
	struct ib_sge sge;
	u64 dma_addr;
	int ret;

	entry = req->entry;
	dma_addr = ib_dma_map_single(ctrl->dev, &entry->info,
			sizeof(entry->info), DMA_FROM_DEVICE);
	if (unlikely(ib_dma_mapping_error(ctrl->dev, dma_addr))) {
		pr_err("dma address mapping error.\n");
		return -1;
	}

	ib_dma_sync_single_for_device(ctrl->dev, dma_addr,
			sizeof(entry->info), DMA_FROM_DEVICE);

	sge.addr = dma_addr;
	sge.length = sizeof(entry->info);
	sge.lkey = ctrl->pd->local_dma_lkey;

	wr.wr_cqe = &req->cqe;
	wr.next = NULL;
	wr.sg_list = &sge;
	wr.num_sge = 1;

	ret = ib_post_recv(ctrl->qp, &wr, &bad_wr);
	if (ret) {
		pr_err("ib_post_recv failed to post RR for page ack (code = %d).\n", ret);
		return ret;
	}

	return 0;
}


// TODO(dimlek): here we should also consider the `swap_type`, since
// we may have several.
/*
static struct page_info* find_page_remote_info(pgoff_t offset) {
	struct page_node *curr;
	hash_for_each_possible(rmem_map, curr, node, offset) {
		if (curr->offset == offset) {
			pr_info("hash table: remote addr for page offset = %lu found.\n", offset);
			return &curr->info;
		}
	}
	pr_info("hash table: did not found remote info for offset = %lu.\n", offset);
	return 0;
}
*/

static int swapmon_store_sync(unsigned swap_type, pgoff_t offset,
												 struct page *page) {
	struct mcswap_tree *tree = mcswap_trees[swap_type];
	struct mcswap_entry* rmem_node, *ent;
	struct page_ack_req* req;
	long wait_ret;
	int ret;

	pr_info("[%u] swapmon: store(swap_type = %u, offset = %lu)\n",
				 ++page_cnt, swap_type, offset);

	rmem_node = kmem_cache_alloc(mcswap_entry_cache, GFP_KERNEL);
	if (unlikely(!rmem_node)) {
		pr_err("kzalloc failed to allocate mem for u64.\n");
		return -ENOMEM;
	}
	rmem_node->offset = offset;

	req = kmem_cache_alloc(rdma_req_cache, GFP_KERNEL);
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
		return -1;
	}

	// multicast the physical page to the memory servers
	ret = swapmon_mcast_send(page, offset);
	if (unlikely(ret)) {
		pr_err("swapmon_mcast_send() failed.\n");
		return -1;
	}

	wait_ret = wait_for_completion_interruptible_timeout(&req->ack,
			msecs_to_jiffies(4) + 1);
	if (unlikely(wait_ret == -ERESTARTSYS)) {
		pr_err("interrupted on ack waiting.\n");
		return wait_ret;
	} else if (unlikely(wait_ret == 0)) {
		pr_err("timed out on ack waiting.\n");
		return -1;
	}

	// once we are here we have successfully received an ACK from mem server
	spin_lock(&tree->lock);
	ret = mcswap_rb_insert(&tree->root, req->entry);
	if (ret) {
		// entry already exists for this offset, so we need to release
		// the lock and free the mcswap_entry structure here.
		spin_unlock(&tree->lock);
		kmem_cache_free(mcswap_entry_cache, req->entry);
	} else {
		// entry was added to the remote memory mapping
		ent = rb_find_page_remote_info(&tree->root, offset);
		spin_unlock(&tree->lock);
		pr_info("[%u] page %lu stored at: remote addr = %llu, rkey = %u\n",
				page_cnt, offset, ent->info.remote_addr, ent->info.rkey);
  }
	kmem_cache_free(rdma_req_cache, req);

	return -1;
}


static int swapmon_store(unsigned swap_type, pgoff_t offset,
												 struct page *page) {
	struct mcswap_entry* rmem_node;
	struct page_ack_req* req;
	int ret;

	page_cnt++;
	if (page_cnt % 64 == 0) {
		mdelay(7);
	}
	/*
	if (page_cnt >= 350) {
		return -1;
	} else {
		page_cnt++;
	}
	*/

	printk("[%u] swapmon: store(swap_type = %u, offset = %lu)\n",
				 page_cnt, swap_type, offset);

	rmem_node = kmem_cache_alloc(mcswap_entry_cache, GFP_KERNEL);
	if (unlikely(!rmem_node)) {
		pr_err("kzalloc failed to allocate mem for u64.\n");
		return -ENOMEM;
	}
	rmem_node->offset = offset;

	req = kmem_cache_alloc(rdma_req_cache, GFP_KERNEL);
	if (unlikely(!req)) {
		pr_err("slab allocator failed to allocate mem for page_ack_req.\n");
		return -ENOMEM;
	}
	req->entry = rmem_node;
	req->cqe.done = page_ack_done;
	req->swap_type = swap_type;

	// post RR for the ACK message to be received from mem server
	ret = post_recv_page_ack_msg(req);
	if (ret) {
		pr_err("post_recv_page_ack_msg failed.\n");
		return -1;
	}

	// multicast the physical page to the memory servers
	ret = swapmon_mcast_send(page, offset);
	if (unlikely(ret)) {
		pr_err("swapmon_mcast_send() failed.\n");
		return -1;
	}

	/*
	while (true) {
		ret = ib_poll_cq(ctrl->send_cq, 1, &wc);
		if (ret < 0) {
			pr_err("polling failed.\n");
			break;
		} else if (ret > 0) {
			if (wc.status != IB_WC_SUCCESS) {
				printk("Polling failed with status %s (work request ID = %llu).\n",
							 ib_wc_status_msg(wc.status), wc.wr_id);
				return wc.status;
			} else if (wc.wr_id == offset) {
				pr_info("received ACK for page %lu\n", offset);
				ret = mcswap_rb_insert(&mcswap_tree, req->entry);
				break;
			}
		}
	}

	struct mcswap_entry *ent;
	// entry was added to the remote memory mapping, so print logs
	ent = rb_find_page_remote_info(&mcswap_tree, offset);
	if (!ent) {
		pr_info("page was not stored properly.\n");
		return -1;
	}
	pr_info("[%u] page %lu stored at: remote addr = %llu, rkey = %u\n",
			page_cnt, offset, ent->info.remote_addr, ent->info.rkey);
	*/

	return -1;
}

struct hack_s {
	struct ib_cqe cqe;
	struct completion req_done;
};

/*
static void remote_page_read_done(struct ib_cq *cq, struct ib_wc *wc) {
	struct hack_s *req;
	pgoff_t offset;
	int ret;

	req = container_of(wc->wr_cqe, struct hack_s, cqe);
	if (unlikely(wc->status != IB_WC_SUCCESS)) {
		pr_err("Polling failed with status: %s.\n",
					 ib_wc_status_msg(wc->status));
		complete(&req->req_done);
		return;
	}
	complete(&req->req_done);
}
*/

static int swapmon_rdma_read(struct page *page,
		struct mcswap_entry *ent) {
	struct ib_send_wr *bad_send_wr;
	struct ib_rdma_wr rdma_wr;
	struct ib_sge sge;
	struct ib_wc wc;
	u64 dma_addr;
	int retries;
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

	/*
	struct hack_s hack;
	init_completion(&hack.req_done);
	hack.cqe.done = remote_page_read_done;
	rdma_wr.wr.wr_cqe = &hack.cqe;
	*/

	/*
	rdma_wr.wr.wr_cqe = (struct ib_cqe *)
		kzalloc(sizeof(struct ib_cqe), GFP_KERNEL);
	if (!rdma_wr.wr.wr_cqe) {
		pr_err("kzalloc() failed to allocate memory for ib_cqe.\n");
		return -ENOMEM;
	}
	*/

	rdma_wr.wr.wr_id = ent->offset;
	rdma_wr.wr.next = NULL;
	rdma_wr.wr.sg_list = &sge;
	rdma_wr.wr.num_sge = 1;
	rdma_wr.wr.opcode = IB_WR_RDMA_READ;
	rdma_wr.wr.send_flags = IB_SEND_SIGNALED;

	rdma_wr.remote_addr = ent->info.remote_addr;
	rdma_wr.rkey = ent->info.rkey;

	ret = ib_post_send(ctrl->qp, &rdma_wr.wr, &bad_send_wr);
	if (ret) {
		pr_err("ib_post_send failed to read remote page.\n");
		return ret;
	}

	// TODO(dimlek): How shall we poll efficiently here and how many times
	// should we poll for completion?
	retries = 30000;
	while (retries > 0) {
		ret = ib_poll_cq(ctrl->qp->send_cq, 1, &wc);
		if (ret > 0) {
			if (unlikely(wc.status != IB_WC_SUCCESS)) {
				pr_info("Polling failed with status %s.\n",
							 ib_wc_status_msg(wc.status));
				return wc.status;
			}
			if (wc.wr_id == ent->offset) {
				break;
			} else {
				pr_err("Received message with unexpected WR ID.\n");
			}
		} else if (ret < 0) {
			pr_err("ib_poll_cq for RDMA read failed.\n");
			return ret;
		}
		retries--;
	}

	if (retries <= 0) {
		return -1;
	}

	return 0;
}

static int swapmon_load(unsigned swap_type, pgoff_t offset,
		struct page *page) {
	struct mcswap_tree *tree = mcswap_trees[swap_type];
	struct mcswap_entry *entry;
	int ret;

	//pr_info("%s(swap_type = %u, offset = %lu)\n",
				 //__func__, swap_type, offset);

	spin_lock(&tree->lock);
	entry = rb_find_page_remote_info(&tree->root, offset);
	if (!entry) {
		pr_err("failed to find page metadata in red-black tree.\n");
		spin_unlock(&tree->lock);
		return -1;
	}
	spin_unlock(&tree->lock);

	ret = swapmon_rdma_read(page, entry);
	if (unlikely(ret)) {
		pr_info("failed to fetch remote page.\n");
		return -1;
	}

	pr_info("swapmon: page %lu loaded from: remote addr = %llu, rkey = %u\n",
			offset, entry->info.remote_addr, entry->info.rkey);
	return 0;
}

static void swapmon_invalidate_page(unsigned swap_type, pgoff_t offset) {
	struct mcswap_tree *tree = mcswap_trees[swap_type];
	int ret;
	pr_info("invalidate_page(swap_type = %u, offset = %lu)\n",
				 swap_type, offset);

	spin_lock(&tree->lock);
	ret = mcswap_rb_erase(&tree->root, offset);
	if (unlikely(ret)) {
		pr_err("failed to erase page with offset = %lu\n", offset);
		return;
	}
	spin_unlock(&tree->lock);
}

static void swapmon_invalidate_area(unsigned swap_type) {
	struct mcswap_tree *tree = mcswap_trees[swap_type];
	struct mcswap_entry *curr, *tmp;
	pr_info("swapmon: invalidate_area(swap_type = %u)\n", swap_type);

	spin_lock(&tree->lock);
	rbtree_postorder_for_each_entry_safe(curr, tmp, &tree->root, node) {
		// TODO(dimlek): the check for empty node might not be needed here.
		if (!RB_EMPTY_NODE(&curr->node)) {
			rb_erase(&curr->node, &tree->root);
			kmem_cache_free(mcswap_entry_cache, curr);
			RB_CLEAR_NODE(&curr->node);
		}
	}
	spin_unlock(&tree->lock);
	kfree(tree);
	mcswap_trees[swap_type] = NULL;
}

static struct frontswap_ops swapmon_fs_ops = {
	// prepares the device to receive frontswap pages associated with the
	// specified swap device number (aka "type"). This function is invoked
	// when we `swapon` a device.
	.init  = swapmon_init,
	// copies the page to transcendent memory and associate it with the type
	// and offset associated with the page.
	.store = swapmon_store_sync,
	// copies the page, if found, from transcendent memory into kernel memory,
	// but will not remove the page from transcendent memory.
	.load  = swapmon_load,
	// removes the page from transcendent memory.
	.invalidate_page = swapmon_invalidate_page,
	// removes all pages from transcendent memory.
	.invalidate_area = swapmon_invalidate_area,
};

/*
static void swapmon_rdma_dev_add(struct ib_device* dev) {
	printk("swapmon: swapmon_rdma_dev_add()\n");
}

static void swapmon_rdma_dev_remove(struct ib_device* dev, void *client_data) {
	printk("swapmon: swapmon_rdma_dev_remove()\n");

	mutex_lock(&swapmon_rdma_ctrl_mutex);
	if (ctrl->dev == dev) {
		dev_info(&(ctrl->dev->dev), "removing ctrl: addr %pISp\n",
				&ctrl->client_addr);
	}
	mutex_unlock(&swapmon_rdma_ctrl_mutex);
}

static struct ib_client swapmon_ib_client = {
	.name   = "swapmon_kernel",
	.add    = swapmon_rdma_dev_add,
	.remove = swapmon_rdma_dev_remove
};
*/

#define MAX_NR_SERVERS 3

/*--------------------- Module parameters ---------------------*/

static char *server[MAX_NR_SERVERS];
static int nr_servers;
module_param_array(server, charp, &nr_servers, 0644);
MODULE_PARM_DESC(server, "IP addresses of memory servers in n.n.n.n form");

static int server_port;
module_param_named(sport, server_port, int, 0644);
MODULE_PARM_DESC(server_port, "Port number that memory servers"
		" are listening on");

static char client_ip[INET_ADDRSTRLEN];
module_param_string(cip, client_ip, INET_ADDRSTRLEN, 0644);
MODULE_PARM_DESC(client_ip, "IP address in n.n.n.n form of the interface used"
		"for communicating with the memory servers");


static int swapmon_rdma_parse_ipaddr(struct sockaddr_in *saddr, char *ip) {
	int ret;
  u8 *addr = (u8 *)&saddr->sin_addr.s_addr;
  size_t buflen = strlen(ip);

  pr_info("start: %s\n", __FUNCTION__);

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
static int swapmon_rdma_create_qp(const int factor) {
	struct ib_qp_init_attr init_attr;
	int ret;

	memset(&init_attr, 0, sizeof(init_attr));
	init_attr.event_handler = swapmon_rdma_qp_event;
	/* +1 for drain */
	init_attr.cap.max_send_wr = factor * ctrl->queue_size + 1;
	/* +1 for drain */
	init_attr.cap.max_recv_wr = 12 * 1024;
	init_attr.cap.max_recv_sge = 1;
	init_attr.cap.max_send_sge = 1;
	init_attr.sq_sig_type = IB_SIGNAL_REQ_WR;
	init_attr.qp_type = IB_QPT_RC;
	init_attr.send_cq = ctrl->send_cq;
	init_attr.recv_cq = ctrl->recv_cq;

	// allocate a queue pair (QP) and associate it with the specified
	// RDMA identifier cm_id
	ret = rdma_create_qp(ctrl->cm_id, ctrl->pd, &init_attr);
	if (ret) {
		pr_err("rdma_create_qp failed.\n");
	}
	ctrl->qp = ctrl->cm_id->qp;

	return ret;
}


static int swapmon_rdma_addr_resolved(void) {
	const int send_wr_factor = 11;
	const int cq_factor = send_wr_factor + 1;
	int comp_vector = 0;
	int ret;

	// struct ib_device represents a handle to the HCA
	ctrl->dev = ctrl->cm_id->device;
	if (!ctrl->dev) {
		pr_err("No device found.\n");
		ret = PTR_ERR(ctrl->dev);
		goto out_err;
	}

	// Allocate a protection domain which we can use to create objects within
	// the domain. we can register and associate memory regions with this PD
	// In addition to the CQs to be associated with the QP about to be created,
	// the PD that the QP will belong to must also have been created.
	ctrl->pd = ib_alloc_pd(ctrl->dev, 0);
	if (IS_ERR(ctrl->pd)) {
		pr_err("ib_alloc_pd failed.\n");
		ret = PTR_ERR(ctrl->pd);
		goto out_err;
	}

	// check whether memory registrations are supported
	if (!(ctrl->dev->attrs.device_cap_flags &
				IB_DEVICE_MEM_MGT_EXTENSIONS)) {
		pr_err("Memory registrations not supported.\n");
		ret = -1;
		goto out_free_pd;
	}

	// Before creating the local QP, we must first cause the HCA to create the CQs
	// to be associated with the SQ and RQ of the QP about to be created. The QP's
	// SQ and RQ can have separate CQs or may share a CQ. The 3rd argument
	// specifies the number of Completion Queue Entries (CQEs) and represents the
	// size of the CQ.
	ctrl->send_cq = ib_alloc_cq(ctrl->dev, ctrl, cq_factor * 1024,
			comp_vector, IB_POLL_SOFTIRQ);
	if (IS_ERR(ctrl->send_cq)) {
		ret = PTR_ERR(ctrl->send_cq);
		goto out_err;
	}

	ctrl->recv_cq = ib_alloc_cq(ctrl->dev, ctrl, cq_factor * 1024,
			comp_vector, IB_POLL_SOFTIRQ);
	if (IS_ERR(ctrl->recv_cq)) {
		ret = PTR_ERR(ctrl->recv_cq);
		goto out_err;
	}

	ret = swapmon_rdma_create_qp(1024);
	if (ret) {
		pr_err("swapmon_rdma_create_qp failed\n");
		goto out_destroy_ib_cq;
	}

	// we now resolve the RDMA address bound to the RDMA identifier into
	// route information needed to establish a connection. This is invoked
	// in the client side of the connection and we must have already resolved
	// the dest address into an RDMA address through `rdma_resolve_addr`.
	ret = rdma_resolve_route(ctrl->cm_id, SWAPMON_RDMA_CONNECTION_TIMEOUT_MS);
	if (ret) {
		pr_err("rdma_resolve_route failed: %d\n", ret);
		goto out_destroy_qp;
	}

	return 0;

out_destroy_qp:
	rdma_destroy_qp(ctrl->cm_id);
out_destroy_ib_cq:
	ib_free_cq(ctrl->send_cq);
out_free_pd:
	ib_dealloc_pd(ctrl->pd);
out_err:
	return ret;
}

static int swapmon_rdma_route_resolved(void) {
	// specify the connection parameters
	struct rdma_conn_param param = {};
  int ret;

  param.private_data = NULL;
  param.private_data_len = 0;
  param.responder_resources = 8;
  param.initiator_depth = 8;
  param.flow_control = 0;
  param.retry_count = 3;
  param.rnr_retry_count = 3;
  param.qp_num = ctrl->qp->qp_num;

  pr_info("max_qp_rd_atom=%d max_qp_init_rd_atom=%d\n",
      ctrl->dev->attrs.max_qp_rd_atom,
      ctrl->dev->attrs.max_qp_init_rd_atom);

	// Initiate an active connection request (i.e. send REQ message). The route
	// must have been resolved before trying to connect.
  ret = rdma_connect(ctrl->cm_id, &param);
  if (ret) {
    pr_err("rdma_connect failed (%d)\n", ret);
    ib_free_cq(ctrl->send_cq);
  }
	pr_info("trying to rdma_connect() ...\n");

  return 0;
}

static int str_to_ib_sockaddr(char *dst, struct sockaddr_ib *mcast_addr) {
	int ret;
	size_t len = strlen(dst);

	pr_info("start: %s\n", __FUNCTION__);
	if (len > INET6_ADDRSTRLEN) {
		return -EINVAL;
	}

	ret = in6_pton(dst, len, mcast_addr->sib_addr.sib_raw, '\0', NULL);
	if (ret <= 0) {
		pr_err("[MCAST]: in6_pton() failed with ret = %d\n", ret);
		return ret;
	}
	mcast_addr->sib_family = AF_IB;

	return 0;
}

static int mcast_logic(void) {
	struct ib_qp_init_attr init_attr;
	int ret;

	pr_info("[MCAST]: mcast_logic()\n");
	if (rdma_cap_ib_mcast(ctrl->dev, ctrl->cm_id->port_num)) {
		// TODO(dimlek): here we must implement the logic which registers the HCA
		// with the Subnet Manager (SM) to join the multicast group.
		pr_info("[MCAST]: rdma_cap_ib_mcast returned true.\n");
	}

	ret = str_to_ib_sockaddr("::", &ctrl->mcast_addr);
	if (ret) {
		pr_err("parse_ib_maddr() failed\n");
		return ret;
	}
	pr_info("[MCAST]: multicast address parsed successfully.\n");

	ctrl->mcast_cm_id = rdma_create_id(&init_net, swapmon_rdma_cm_event_handler,
			ctrl, RDMA_PS_UDP, IB_QPT_UD);
	if (IS_ERR(ctrl->mcast_cm_id)) {
		pr_err("[MCAST]: CM ID creation failed\n");
		return -ENODEV;
	}
	pr_info("[MCAST]: rdma_create_id completed successfully.\n");

	ret = rdma_bind_addr(ctrl->mcast_cm_id,
			(struct sockaddr *) &ctrl->client_addr);
	if (ret) {
		pr_err("[MCAST]: rdma_bind_addr failed: %d\n", ret);
		return ret;
	}

	ctrl->mcast_pd = ib_alloc_pd(ctrl->dev, 0);
	if (IS_ERR(ctrl->mcast_pd)) {
		pr_err("ib_alloc_pd failed.\n");
		return PTR_ERR(ctrl->mcast_pd);
	}
	pr_info("[MCAST]: ib_alloc_pd completed successfully.\n");

	ctrl->mcast_cq = ib_alloc_cq(ctrl->dev, ctrl, 12288, 0, IB_POLL_SOFTIRQ);
	if (IS_ERR(ctrl->mcast_cq)) {
		ret = PTR_ERR(ctrl->mcast_cq);
	}
	pr_info("[MCAST]: ib_alloc_cq completed successfully.\n");

	// TODO(dimlek): set max_send/recv properly
	memset(&init_attr, 0, sizeof(init_attr));
	init_attr.event_handler = swapmon_rdma_qp_event;
	init_attr.cap.max_send_wr = 12 * 1024;
	init_attr.cap.max_send_sge = 1;
	init_attr.cap.max_recv_wr = 0;
	init_attr.cap.max_recv_sge = 0;
	init_attr.sq_sig_type = IB_SIGNAL_REQ_WR;
	init_attr.qp_type = IB_QPT_UD;
	init_attr.send_cq = ctrl->mcast_cq;
	init_attr.recv_cq = ctrl->mcast_cq;

	// allocate a queue pair (QP) and associate it with the specified
	// RDMA identifier cm_id
	ret = rdma_create_qp(ctrl->mcast_cm_id, ctrl->mcast_pd, &init_attr);
	if (ret) {
		pr_err("rdma_create_qp failed.\n");
		return -1;
	}
	ctrl->mcast_qp = ctrl->mcast_cm_id->qp;
	pr_info("[MCAST]: rdma_create_qp completed succesfully.\n");

	ret = rdma_join_multicast(ctrl->mcast_cm_id,
			(struct sockaddr *) &ctrl->mcast_addr, BIT(FULLMEMBER_JOIN), ctrl);
	if (ret) {
		pr_err("rdma_join_multicast failed.\n");
		return -2;
	}
	pr_info("[MCAST]: rdma_join_multicast completed succesfully.\n");

	return 0;
}

/*
#define UD_DUMMY_MSG "hello UD world!"
static int send_dummy_ud_msg(void) {
	struct ib_ud_wr ud_wr = {};
	// struct ib_send_wr *bad_wr;
	struct ib_sge sg;
	// struct ib_wc wc;
	u64 dma_addr;
	// int ret;

	char* msg;
	msg = kzalloc(sizeof(UD_DUMMY_MSG)+1, GFP_KERNEL);
	if (!msg) {
		pr_err("kzalloc failed to allocate mem for dummy UD msg.\n");
		return -ENOMEM;
	}

	dma_addr = ib_dma_map_single(ctrl->dev, msg,
			sizeof(UD_DUMMY_MSG)+1, DMA_TO_DEVICE);
	if (ib_dma_mapping_error(ctrl->dev, dma_addr)) {
		pr_err("dma address mapping error.\n");
	}

	// Prepare DMA region to be accessed by device.
	ib_dma_sync_single_for_device(ctrl->dev, dma_addr,
			sizeof(UD_DUMMY_MSG)+1, DMA_TO_DEVICE);

	sg.addr = dma_addr;
	sg.length = sizeof(UD_DUMMY_MSG)+1;
	sg.lkey = ctrl->pd->local_dma_lkey;

	ud_wr.wr.next = NULL;
	ud_wr.wr.wr_id = 1000;
	ud_wr.wr.sg_list = NULL;
	ud_wr.wr.num_sge = 0;
	ud_wr.wr.opcode = IB_WR_SEND;
	ud_wr.wr.send_flags = IB_SEND_SIGNALED;

	ud_wr.ah = ctrl->ah;
	ud_wr.remote_qpn = ctrl->server.ud_transport.qpn;
	ud_wr.remote_qkey = ctrl->server.ud_transport.qkey;

	ret = ib_post_send(ctrl->ud_qp, &ud_wr.wr, &bad_wr);
	if (ret) {
		pr_err("ib_post_send failed to send dummy msg.\n");
	}

	pr_info("send_dummy_ud_msg completed.\n");
	return 0;
}
*/


static int init_ud_comms(void) {
	struct ib_qp_attr qp_attr;
	struct ib_qp_init_attr qp_init_attr = {};
	struct rdma_ah_attr ah_attr;
	union ib_gid sgid;
	u16 sgid_index;
	int ret;

	qp_init_attr.send_cq = ctrl->send_cq;
	qp_init_attr.recv_cq = ctrl->recv_cq;
	qp_init_attr.cap.max_send_wr  = 128;
	qp_init_attr.cap.max_recv_wr  = 0;
	qp_init_attr.cap.max_send_sge = 1;
	qp_init_attr.cap.max_recv_sge = 0;
	qp_init_attr.qp_type = IB_QPT_UD;

	ctrl->ud_qp = ib_create_qp(ctrl->pd, &qp_init_attr);
	if (IS_ERR(ctrl->ud_qp)) {
		pr_err("ib_create_qp for UD communication failed.\n");
		return PTR_ERR(ctrl->ud_qp);
	}

	memset(&qp_attr, 0, sizeof(qp_attr));
	qp_attr.qp_state   = IB_QPS_INIT;

	ret = ib_find_pkey(ctrl->dev, ctrl->cm_id->port_num,
				ctrl->server.ud_transport.pkey, &qp_attr.pkey_index);
	if (ret) {
		pr_err("ib_find_pkey (port_num=%u,pkey=%u) failed.\n",
				ctrl->cm_id->port_num, ctrl->server.ud_transport.pkey);
		return ret;
	}
	qp_attr.port_num = ctrl->cm_id->port_num;
	qp_attr.qkey = ctrl->server.ud_transport.qkey;

	ret = ib_modify_qp(ctrl->ud_qp, &qp_attr,
			IB_QP_STATE | IB_QP_PKEY_INDEX | IB_QP_PORT | IB_QP_QKEY);
	if (ret) {
		pr_err("ib_modify_qp failed to move QP to state INIT.\n");
		return ret;
	}

	pr_info("[QP - INIT]port_num = %u, qkey = %u, pkey_index = %u\n",
			qp_attr.port_num, qp_attr.qkey, qp_attr.pkey_index);

	memset(&qp_attr, 0, sizeof(qp_attr));
	qp_attr.qp_state = IB_QPS_RTR;
	ret = ib_modify_qp(ctrl->ud_qp, &qp_attr, IB_QP_STATE);
	if (ret) {
		pr_err("ib_modify_qp failed to move QP to state RTR.\n");
		return ret;
	}

	memset(&qp_attr, 0, sizeof(qp_attr));
	qp_attr.qp_state = IB_QPS_RTS;
	// TODO(dimlek): check whether `sq_psn` should be set otherwise
	qp_attr.sq_psn = 0;

	ret = ib_modify_qp(ctrl->ud_qp, &qp_attr, IB_QP_STATE | IB_QP_SQ_PSN);
	if (ret) {
		pr_err("ib_modify_qp failed to move QP to state RTS.\n");
		return ret;
	}
	pr_info("UD QP is in RTS state.\n");

	sgid = ctrl->cm_id->route.path_rec->sgid;
	ret = ib_find_gid(ctrl->dev, &sgid, IB_GID_TYPE_IB, NULL,
			&ctrl->cm_id->port_num, &sgid_index);
	if (ret) {
		pr_err("ib_find_gid failed.\n");
		return ret;
	}

	memset(&ah_attr, 0, sizeof(ah_attr));
	ah_attr.type = RDMA_AH_ATTR_TYPE_IB;

	rdma_ah_set_grh(&ah_attr, &ctrl->server.ud_transport.gid,
			0 /* flow_label */, sgid_index /* sgid_index */,
			1 /* hop_limit */, 0 /* traffic_class */);
	rdma_ah_set_sl(&ah_attr, 0);
	rdma_ah_set_static_rate(&ah_attr, 0);
	rdma_ah_set_port_num(&ah_attr, ctrl->cm_id->port_num);
	rdma_ah_set_ah_flags(&ah_attr, IB_AH_GRH); // use global routing

	rdma_ah_set_dlid(&ah_attr, ctrl->server.ud_transport.lid);
	rdma_ah_set_path_bits(&ah_attr, 0);

	ctrl->ah = rdma_create_ah(ctrl->pd, &ah_attr);
	if (IS_ERR(ctrl->ah)) {
		pr_err("rdma_create_ah failed.\n");
		return PTR_ERR(ctrl->ah);
	}

	pr_info("address handle created succesfully.\n");
	return 0;
}


static int send_dummy_mcast_msg(void) {
	struct ib_ud_wr ud_wr;
	struct ib_send_wr *bad_wr;
	struct ib_sge sg;
	struct ib_wc wc;
	u64 dma_addr;
	char* msg;
	size_t msg_len;
	int ret;

	msg_len = sizeof("Hello mcast world!");
	msg = kzalloc(msg_len, GFP_KERNEL);
	if (!msg) {
		pr_err("kzalloc failed to allocate mem for dummy mcast msg.\n");
		return -ENOMEM;
	}
	strcpy(msg, "Hello mcast world!");

	pr_info("send_dummy_mcast_msg(): kzalloc() and strcpy() for msg success.\n");
	pr_info("msg_len = %lu\n", msg_len);

	dma_addr = ib_dma_map_single(ctrl->dev, msg,
			msg_len, DMA_BIDIRECTIONAL);
	if (ib_dma_mapping_error(ctrl->dev, dma_addr)) {
		pr_err("dma address mapping error.\n");
	}

	// Prepare DMA region to be accessed by device.
	ib_dma_sync_single_for_device(ctrl->dev, dma_addr,
			msg_len, DMA_BIDIRECTIONAL);

	pr_info("send_dummy_mcast_msg(): dma mappings ready.\n");

	sg.addr = dma_addr;
	sg.length = msg_len;
	sg.lkey = ctrl->mcast_pd->local_dma_lkey;

	ud_wr.wr.next = NULL;
	ud_wr.wr.wr_id = 420;
	ud_wr.wr.sg_list = &sg;
	ud_wr.wr.num_sge = 1;
	ud_wr.wr.opcode = IB_WR_SEND; //_WITH_IMM;
	ud_wr.wr.send_flags = IB_SEND_SIGNALED;
	// ud_wr.wr.ex.imm_data = htonl(ctrl->mcast_qp->qp_num);

	ud_wr.ah = ctrl->mcast_ah;
	ud_wr.remote_qpn = ctrl->remote_mcast_qpn;
	ud_wr.remote_qkey = ctrl->remote_mcast_qkey;

	ret = ib_post_send(ctrl->mcast_qp, &ud_wr.wr, &bad_wr);
	if (ret) {
		pr_err("ib_post_send failed to send dummy msg.\n");
	}

	while (!ib_poll_cq(ctrl->mcast_qp->send_cq, 1, &wc)) {
		// nothing
	}

	if (wc.status != IB_WC_SUCCESS) {
		printk("Polling failed with status %s (work request ID = %llu).\n",
					 ib_wc_status_msg(wc.status), wc.wr_id);
		return wc.status;
	}
	pr_info("send_dummy_mcast_msg completed.\n");

	return 0;
}

static int send_mcast_grp_info(struct ib_qp *qp, struct rdma_ud_param *param) {
	struct ib_send_wr *bad_wr;
	struct ib_send_wr wr;
	struct ib_sge sge;
	struct ib_wc wc;
	int ret;

	struct mcast_grp_info {
		u8 gid_raw[16];
		u16 lid;
		u32 qkey;
	};
	struct mcast_grp_info *msg;

	u64 dma_addr;
	/*
	ib_dma_alloc_coherent(ctrl->dev, sizeof(struct mcast_grp_info),
			&dma_addr, GFP_KERNEL);

	memcpy(((struct mcast_grp_info *) dma_addr)->gid_raw,
			param->ah_attr.grh.dgid.raw, 16);
	((struct mcast_grp_info *) dma_addr)->lid  = param->ah_attr.ib.dlid;
	((struct mcast_grp_info *) dma_addr)->qkey = param->qkey;


	// Prepare DMA region to be accessed by device.
	ib_dma_sync_single_for_device(ctrl->dev, dma_addr,
			sizeof(struct mcast_grp_info), DMA_TO_DEVICE);
	*/

	msg = kzalloc(sizeof(struct mcast_grp_info), GFP_KERNEL);
	if (!msg) {
		pr_err("kzalloc failed to allocate mem for struct mcast_grp_info.\n");
		return -ENOMEM;
	}

	pr_info("send_mcast_grp_info() init.\n");

	memcpy(msg->gid_raw, param->ah_attr.grh.dgid.raw, 16);
	msg->lid = param->ah_attr.ib.dlid;
	msg->qkey = param->qkey;

	dma_addr = ib_dma_map_single(ctrl->dev, msg,
			sizeof(struct mcast_grp_info), DMA_BIDIRECTIONAL);
	if (ib_dma_mapping_error(ctrl->dev, dma_addr)) {
		pr_err("dma address mapping error.\n");
	}

	// Prepare DMA region to be accessed by device.
	ib_dma_sync_single_for_device(ctrl->dev, dma_addr,
			sizeof(struct mcast_grp_info), DMA_BIDIRECTIONAL);

	sge.addr = dma_addr;
	sge.length = sizeof(struct mcast_grp_info);
	sge.lkey = ctrl->pd->local_dma_lkey;

	wr.next = NULL;
	wr.wr_id = 200;
	wr.sg_list = &sge;
	wr.num_sge = 1;
	wr.opcode = IB_WR_SEND;
	wr.send_flags = IB_SEND_SIGNALED;

	pr_info("ib_post_send() init.\n");
	ret = ib_post_send(qp, &wr, &bad_wr);
	if (ret) {
		pr_err("ib_post_send() failed with code = %d\n.", ret);
		return ret;
	}
	pr_info("ib_post_send() completed.\n");

	while (!ib_poll_cq(qp->send_cq, 1, &wc)) {
		// nothing
	}

	if (wc.status != IB_WC_SUCCESS) {
		printk("Polling failed with status %s (work request ID = %llu).\n",
					 ib_wc_status_msg(wc.status), wc.wr_id);
		return wc.status;
	}
	pr_info("wc succeeded wr_id = %llu", wc.wr_id);

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

	ctrl->server.mem.addr = msg->addr;
	ctrl->server.mem.len  = msg->len;
	ctrl->server.mem.key  = msg->key;
	pr_info("Remote mem info: addr = %llu, len = %u, key = %u\n",
			ctrl->server.mem.addr, ctrl->server.mem.len, ctrl->server.mem.key);

	ctrl->server.ud_transport.qpn  = msg->qpn;
	ctrl->server.ud_transport.gid  = ctrl->cm_id->route.path_rec->dgid;
	ctrl->server.ud_transport.lid  = wc.slid;
	ctrl->server.ud_transport.qkey = UC_QKEY;
	ctrl->server.ud_transport.pkey = IB_DEFAULT_PKEY_FULL;
	pr_info("UD transport info: dgid = %16phC, dlid = 0x%x, "
			"qpn = %u,  qkey= %u, pkey = %u\n",
			ctrl->server.ud_transport.gid.raw, ctrl->server.ud_transport.lid,
			ctrl->server.ud_transport.qpn, ctrl->server.ud_transport.qkey,
			ctrl->server.ud_transport.pkey);

	return 0;
}

static int mcast_join_handler(struct rdma_ud_param *param) {
	if (param->ah_attr.type != RDMA_AH_ATTR_TYPE_IB) {
		pr_err("Only IB AH type is currently supported.\n");
		return -1;
	}

	pr_info("UD Multicast membership info: dgid = %16phC, mlid = 0x%x, "
			"sl = %d, src_path_bits = %u, qpn = %u, qkey = %u\n",
			param->ah_attr.grh.dgid.raw, param->ah_attr.ib.dlid,
			rdma_ah_get_sl(&param->ah_attr), rdma_ah_get_path_bits(&param->ah_attr),
			param->qp_num, param->qkey);

	ctrl->remote_mcast_qpn = param->qp_num;
	ctrl->remote_mcast_qkey = param->qkey;

	send_mcast_grp_info(ctrl->qp, param);

	ctrl->mcast_ah = rdma_create_ah(ctrl->mcast_pd, &param->ah_attr);
	if (!ctrl->mcast_ah) {
		pr_err("rdma_create_ah() failed for multicast AH.\n");
		return PTR_ERR(ctrl->mcast_ah);
	}

	pr_info("mcast_join_handler done.\n");

	return 0;
}


static void recv_control_msg_done(struct ib_cq* cq, struct ib_wc* wc) {
	enum comm_ctrl_opcode op;
	pr_info("%s()\n", __func__);
	if (unlikely(wc->status != IB_WC_SUCCESS)) {
		pr_err("control message WC failed with status: %s\n",
				ib_wc_status_msg(wc->status));
		return;
	}

	if (!(wc->wc_flags & IB_WC_WITH_IMM)) {
		pr_err("control message should always contain IMM data.\n");
		return;
	}

	op = ntohl(wc->ex.imm_data);
	switch(op) {
		case MCAST_MEMBERSHIP_ACK:
			pr_info("Mem server joined multicast group.\n");
			complete(&ctrl->mcast_ack);
			break;
		case MCAST_MEMBERSHIP_NACK:
			pr_info("Mem server failed to join multicast group.\n");
			break;
		default:
			pr_err("Unrecognized control message with code: %u\n", op);
			break;
	}
}

// This function tries to post `num_recvs` Receive Requests (RR) and
// returns the number of RRs that it posted successfull.
static int post_recv_control_msg(int num_recvs) {
	struct ib_recv_wr wr, *bad_wr;
	int i, ret;

	wr.next    = NULL;
	wr.num_sge = 0;
	wr.sg_list = NULL;

	wr.wr_cqe = (struct ib_cqe *) kzalloc(sizeof(struct ib_cqe), GFP_KERNEL);
	if (!wr.wr_cqe) {
		pr_err("kzalloc() failed to allocate memory for ib_cqe.\n");
		return 0;
	}
	wr.wr_cqe->done = recv_control_msg_done;

	for (i = 0; i < num_recvs; i++) {
		ret = ib_post_recv(ctrl->qp, &wr, &bad_wr);
		if (unlikely(ret)) {
			pr_info("%s: ib_post_recv() failed with exit code = %d\n", __func__, ret);
			break;
		}
	}

	pr_info("%s(%d) completed successfully.\n", __func__, num_recvs);
	return i;
}

// For connection establishment, a sample sequence consists of a Request (REQ)
// from the client to the server, a Reply (REP) from the server to the client
// (to indicate that the request was accepted), and a Ready To Use (RTU) from
// the client to the server.
static int swapmon_rdma_cm_event_handler(struct rdma_cm_id *id,
		struct rdma_cm_event *event) {
	struct ib_port_attr port_attr;
	int ret;
	int cm_error = 0;
	printk("rdma_cm_event_handler msg: %s (%d) status %d id $%p\n",
				 rdma_event_msg(event->event), event->event, event->status, id);

	switch (event->event) {
		case RDMA_CM_EVENT_ADDR_RESOLVED:
			cm_error = swapmon_rdma_addr_resolved();
			break;
		case RDMA_CM_EVENT_ROUTE_RESOLVED:
			cm_error = swapmon_rdma_route_resolved();
			break;
		case RDMA_CM_EVENT_ESTABLISHED:
			ret = ib_query_port(ctrl->dev, ctrl->cm_id->port_num, &port_attr);
			if (ret) {
				pr_err("ib_query_port failed to query port_num = %u\n",
						ctrl->cm_id->port_num);
			}
			if (port_attr.active_mtu != IB_MTU_4096) {
				pr_err("port_num = %u needs active MTU = %u (current MTU = %u)",
						ctrl->cm_id->port_num, ib_mtu_enum_to_int(IB_MTU_4096),
						ib_mtu_enum_to_int(port_attr.active_mtu));
			}

			pr_info("connection established successfully\n");
			// Wait to receive memory info to retrieve all required info to properly
			// reference remote memory.
			ret = recv_mem_info(id->qp);
			if (ret) {
				pr_err("unable to receive remote mem info.\n");
			}
			mcast_logic();
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
			post_recv_control_msg(1);
			cm_error = mcast_join_handler(&event->param.ud);
			pr_info("multicast join operation completed successfully.\n");
			ctrl->cm_error = 0;
			complete(&ctrl->cm_done);
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

	pr_info("cm_error=%d\n", cm_error);

	if (cm_error) {
		ctrl->cm_error = cm_error;
		complete(&ctrl->cm_done);
	}

	return 0;
}

static void swapmon_wait_for_mcast_ack(void) {
	wait_for_completion_interruptible_timeout(&ctrl->mcast_ack,
			msecs_to_jiffies(SWAPMON_MCAST_JOIN_TIMEOUT_MS) + 1);
}

static int swapmon_rdma_wait_for_cm(void) {
	wait_for_completion_interruptible_timeout(&ctrl->cm_done,
			msecs_to_jiffies(SWAPMON_RDMA_CONNECTION_TIMEOUT_MS) + 1);
	return ctrl->cm_error;
}


static int swapmon_rdma_create_ctrl(void) {
	int ret;
	int i;

	printk("swapmon: swapmon_rdma_create_ctrl()\n");

	if (nr_servers > MAX_NR_SERVERS) {
		pr_err("Maximum number of supported memory servers: %d\n", MAX_NR_SERVERS);
		return -1;
	}

	// once we have established a protection domain (PD), we may create objects
	// within that domain. In a PD, we can register memory regions (MR), create
	// queue pairs (QP) and create address handles (AH).
	// ib_alloc_pd();

	ctrl = kzalloc(sizeof(struct swapmon_rdma_ctrl), GFP_KERNEL);
	if (!ctrl)
		return -ENOMEM;
	ctrl->qp_type = IB_QPT_RC;
	ctrl->queue_size = 4;

	for (i = 0; i < nr_servers; i++) {
		ret = swapmon_rdma_parse_ipaddr(&(ctrl->server_addr), server[i]);
		if (ret) {
			printk("swapmon: swapmon_rdma_parse_ipaddr() for server ip failed\n");
			return -EINVAL;
		}
		ctrl->server_addr.sin_port = cpu_to_be16(server_port);
		ctrl->server_addr.sin_port = cpu_to_be16(10000);

		ret = swapmon_rdma_parse_ipaddr(&(ctrl->client_addr), client_ip);
		if (ret) {
			printk("swapmon: swapmon_rdma_parse_ipaddr() for client ip failed\n");
			return -EINVAL;
		}
		pr_info("src and dst addresses parsed successfully.\n");

		init_completion(&ctrl->cm_done);
		init_completion(&ctrl->mcast_ack);
		atomic_set(&ctrl->pending, 0);
		spin_lock_init(&ctrl->cq_lock);

		pr_info("init_completion, atomic_set and spin_lock_init successful.\n");

		// Each end of an RC service requires a Connection Manager (CM) to exchange
		// Management Datagram (MAD) in order to create and associate QPs at the two
		// ends. The `rdma_cm` identifier will report its event data (e.g. results of
		// connecting) on the event channel and those events will be handled by the
		// provided callback function `swapmon_rdma_cm_event_handler`.
		ctrl->cm_id = rdma_create_id(&init_net, swapmon_rdma_cm_event_handler, ctrl,
				RDMA_PS_TCP, ctrl->qp_type);
		if (IS_ERR(ctrl->cm_id)) {
			pr_err("RDMA CM ID creation failed\n");
			return -ENODEV;
		}
		pr_info("rdma_create_id completed successfully.\n");

		// Resolve destination and optionally source addresses from IP addresses to an
		// RDMA address and if the resolution is successful, then the RDMA cm id will
		// be bound to an RDMA device.
		ret = rdma_resolve_addr(ctrl->cm_id, (struct sockaddr *)&ctrl->client_addr,
				(struct sockaddr *)&ctrl->server_addr, SWAPMON_RDMA_CONNECTION_TIMEOUT_MS);
		if (ret) {
			pr_err("rdma_resolve_addr failed: %d\n", ret);
			goto out_destroy_cm_id;
		}

		pr_info("rdma_resolve_addr completed successfully.\n");

		ret = swapmon_rdma_wait_for_cm();
		if (ret) {
			pr_err("swapmon_rdma_wait_for_cm failed\n");
			goto out_destroy_cm_id;
		}
	}

	return 0;

out_destroy_cm_id:
	rdma_destroy_id(ctrl->cm_id);
	return ret;
}

static int rdma_conn_init(void) {
	int ret;

	// register an IB client, in order to register callbacks for IB device
	// addition and removal. When an IB device is added, each registered
	// client's add method will be called and similarly for removal.
	/*
	ret = ib_register_client(&swapmon_ib_client);
	if (ret) {
		printk("swapmon: ib_register_client() failed\n");
		return ret;
	}
	printk("ib_register_client() completed successfully.\n");
	*/

	ret = swapmon_rdma_create_ctrl();
	if (ret)
		goto err_unreg_client;

	// make sure that remote server has joined the mcast group
	swapmon_wait_for_mcast_ack();

	// send dummy message since remote peer joined
	send_dummy_mcast_msg();

	return 0;

err_unreg_client:
	// ib_unregister_client(&swapmon_ib_client);
	return ret;
}

static int __init swapmonfs_init(void) {
	int ret;

	mcswap_entry_cache = KMEM_CACHE(mcswap_entry, 0);
	if (unlikely(!mcswap_entry_cache)) {
		pr_err("failed to create slab cache for mcswap entries.\n");
		return -1;
	}

	rdma_req_cache = KMEM_CACHE(page_ack_req, 0);
	if (unlikely(!rdma_req_cache)) {
		pr_err("failed to create slab cache for page ack req entries.\n");
		return -1;
	}

	ret = rdma_conn_init();
	if (!ret) {
		printk("rdma connection established successfully\n");
	}

	frontswap_register_ops(&swapmon_fs_ops);
	pr_info("module loaded successfully.\n");
	return 0;
}

static void __exit swapmonfs_exit(void) {
	kmem_cache_destroy(mcswap_entry_cache);
	pr_info("module unloaded\n");
}

module_init(swapmonfs_init);
module_exit(swapmonfs_exit);

MODULE_AUTHOR("Dimitris Lekkas, dlekkasp@gmail.com");
MODULE_DESCRIPTION("Low latency and memory efficient paging over RDMA");
MODULE_LICENSE("GPL v2");
