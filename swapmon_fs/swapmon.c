#include <linux/module.h>
#include <linux/frontswap.h>
#include <linux/vmalloc.h>
#include <linux/page-flags.h>
#include <linux/memcontrol.h>
#include <linux/smp.h>
#include <linux/inet.h>

#include <rdma/ib_verbs.h>
#include <rdma/rdma_cm.h>

#include <linux/init.h>

#define SWAPMON_RDMA_CONNECTION_TIMEOUT_MS 3000


static int swapmon_rdma_cm_event_handler(struct rdma_cm_id *id,
		struct rdma_cm_event *event);

static DEFINE_MUTEX(swapmon_rdma_ctrl_mutex);

struct rm_info {
	u64 addr;
	u32 len;
	u32 key;
};

struct remote_peer_info {
	union ib_gid gid;
	u32 lid;

	u32 qpn;
	// use ib_find_pkey() to find the index of a given pkey
	u32 qkey;
	// default IB_DEFAULT_PKEY_FULL -> defined in <rdma/ib_mad.h>
	u32 pkey;
};

struct swapmon_rdma_ctrl {
	struct ib_device *dev;

	// protection domain (PD)
	struct ib_pd *pd;
	// queue pair (QP)
	struct ib_qp *qp;
	// completion queue (CQ)
	struct ib_cq *cq;

	// remote memory info
	struct rm_info *rmem;

	struct remote_peer_info rpeer;

	struct ib_ah *ah;

	spinlock_t cq_lock;

	enum ib_qp_type qp_type;

	// RDMA identifier created through `rdma_create_id`
	struct rdma_cm_id *cm_id;

	struct completion cm_done;
	atomic_t pending;
	int cm_error;

	int queue_size;

	// RDMA connection state
	enum rdma_cm_event_type state;

	struct sockaddr_in server_addr;
	struct sockaddr_in client_addr;
};

struct swapmon_transport_ops {
	struct swapmon_rdma_ctrl *(*create_ctrl)(struct device *dev);
};

struct swapmon_rdma_ctrl* ctrl;


static void swapmon_init(unsigned swap_type) {
	printk("swapmon_init(%d)", swap_type);
}

static int swapmon_store(unsigned swap_type, pgoff_t offset,
												 struct page *page) {
	printk("swapmon: store(swap_type = %-10u, offset = %lu)\n",
				 swap_type, offset);
	return -1;
}

static int swapmon_load(unsigned swap_type, pgoff_t offset,
		struct page *page) {
	printk("swapmon: load(swap_type = %-10u, offset = %lu)\n",
				 swap_type, offset);
	return -1;
}

static void swapmon_invalidate_page(unsigned swap_type, pgoff_t offset) {
	printk("swapmon: invalidate_page(swap_type = %-10u, offset = %lu)\n",
				 swap_type, offset);
}

static void swapmon_invalidate_area(unsigned swap_type) {
	printk("swapmon: invalidate_area(swap_type = %-10u)\n", swap_type);
}

static struct frontswap_ops swapmon_fs_ops = {
	// prepares the device to receive frontswap pages associated with the
	// specified swap device number (aka "type"). This function is invoked
	// when we `swapon` a device.
	.init  = swapmon_init,
	// copies the page to transcendent memory and associate it with the type
	// and offset associated with the page.
	.store = swapmon_store,
	// copies the page, if found, from transcendent memory into kernel memory,
	// but will not remove the page from transcendent memory.
	.load  = swapmon_load,
	// removes the page from transcendent memory.
	.invalidate_page = swapmon_invalidate_page,
	// removes all pages from transcendent memory.
	.invalidate_area = swapmon_invalidate_area,
};

static void swapmon_rdma_dev_add(struct ib_device* dev) {
	printk("swapmon: swapmon_rdma_dev_add()\n");
}

static void swapmon_rdma_dev_remove(struct ib_device* dev, void *client_data) {
	printk("swapmon: swapmon_rdma_dev_remove()\n");

	/* delete the controller using this device */
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

static char server_ip[INET_ADDRSTRLEN];
static char client_ip[INET_ADDRSTRLEN];
static int server_port;

module_param_named(sport, server_port, int, 0644);
module_param_string(sip, server_ip, INET_ADDRSTRLEN, 0644);
module_param_string(cip, client_ip, INET_ADDRSTRLEN, 0644);

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
	init_attr.cap.max_recv_wr = ctrl->queue_size + 1;
	init_attr.cap.max_recv_sge = 1;
	init_attr.cap.max_send_sge = 1;
	init_attr.sq_sig_type = IB_SIGNAL_REQ_WR;
	init_attr.qp_type = IB_QPT_RC;
	init_attr.send_cq = ctrl->cq;
	init_attr.recv_cq = ctrl->cq;

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
	const int send_wr_factor = 3;
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
	ctrl->cq = ib_alloc_cq(ctrl->dev, ctrl, cq_factor * 1024, comp_vector,
			IB_POLL_SOFTIRQ);
	if (IS_ERR(ctrl->cq)) {
		ret = PTR_ERR(ctrl->cq);
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
	ib_free_cq(ctrl->cq);
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
    ib_free_cq(ctrl->cq);
  }
	pr_info("trying to rdma_connect() ...\n");

  return 0;
}

static int recv_mem_info(struct ib_qp *qp) {
	struct ib_recv_wr *bad_wr;
	struct ib_recv_wr wr;
	struct ib_sge sge;
	struct ib_wc wc;
	u64 dma_addr;
	int ret;

	ctrl->rmem = kzalloc(sizeof(struct rm_info), GFP_KERNEL);
	if (!ctrl->rmem) {
		pr_err("kzalloc failed to allocate mem for struct rm_info.\n");
		return -ENOMEM;
	}

	// Map kernel virtual address `ctrl->rmem` to a DMA address.
	dma_addr = ib_dma_map_single(ctrl->dev, ctrl->rmem,
			sizeof(struct rm_info), DMA_FROM_DEVICE);
	if (ib_dma_mapping_error(ctrl->dev, dma_addr)) {
		pr_err("dma address mapping error.\n");
	}

	// Prepare DMA region to be accessed by device.
	ib_dma_sync_single_for_device(ctrl->dev, dma_addr,
			sizeof(struct rm_info), DMA_FROM_DEVICE);

	sge.addr = dma_addr;
	sge.length = sizeof(struct rm_info);
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
	printk("received remote mem info: addr = %llu, len = %u, key = %u\n",
				 ctrl->rmem->addr, ctrl->rmem->len, ctrl->rmem->key);

	if (wc.status != IB_WC_SUCCESS) {
		printk("Polling failed with status %s (work request ID = %llu).\n",
					 ib_wc_status_msg(wc.status), wc.wr_id);
		return wc.status;
	}



	return 0;
}

// For connection establishment, a sample sequence consists of a Request (REQ)
// from the client to the server, a Reply (REP) from the server to the client
// (to indicate that the request was accepted), and a Ready To Use (RTU) from
// the client to the server.
static int swapmon_rdma_cm_event_handler(struct rdma_cm_id *id,
		struct rdma_cm_event *event) {
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
			pr_info("connection established successfully\n");
			// Wait to receive memory info to retrieve all required info to properly
			// reference remote memory.
			ret = recv_mem_info(id->qp);
			if (ret) {
				pr_err("unable to receive remote mem info.\n");
			}

			ctrl->cm_error = 0;
			complete(&ctrl->cm_done);
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
		case RDMA_CM_EVENT_DEVICE_REMOVAL:
		case RDMA_CM_EVENT_MULTICAST_JOIN:
			pr_info("multicast join operation completed successfully.\n");
			break;
		case RDMA_CM_EVENT_MULTICAST_ERROR:
		case RDMA_CM_EVENT_ADDR_CHANGE:
		case RDMA_CM_EVENT_TIMEWAIT_EXIT:
			break;
		default:
			pr_err("received unrecognized RDMA CM event %d\n", event->event);
			break;
	}

	if (cm_error) {
		ctrl->cm_error = cm_error;
		complete(&ctrl->cm_done);
	}

	return 0;
}


static int swapmon_rdma_wait_for_cm(void) {
	wait_for_completion_interruptible_timeout(&ctrl->cm_done,
			msecs_to_jiffies(SWAPMON_RDMA_CONNECTION_TIMEOUT_MS) + 1);
	return ctrl->cm_error;
}


static int swapmon_rdma_create_ctrl(void) {
	int ret;
	printk("swapmon: swapmon_rdma_create_ctrl()\n");

	// once we have established a protection domain (PD), we may create objects
	// within that domain. In a PD, we can register memory regions (MR), create
	// queue pairs (QP) and create address handles (AH).
	// ib_alloc_pd();

	ctrl = kzalloc(sizeof(struct swapmon_rdma_ctrl), GFP_KERNEL);
	if (!ctrl)
		return -ENOMEM;
	ctrl->qp_type = IB_QPT_RC;
	ctrl->queue_size = 4;

	ret = swapmon_rdma_parse_ipaddr(&(ctrl->server_addr), server_ip);
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
	// TODO(dimlek): remove this ret statement when everything is ok.
	// return 0;

	ret = swapmon_rdma_wait_for_cm();
	if (ret) {
		pr_err("swapmon_rdma_wait_for_cm failed\n");
		goto out_destroy_cm_id;
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

	return 0;

err_unreg_client:
	// ib_unregister_client(&swapmon_ib_client);
	return ret;
}

static int __init swapmonfs_init(void) {
	int ret;
	frontswap_register_ops(&swapmon_fs_ops);
	printk("swapmon: registered frontswap ops\n");
	ret = rdma_conn_init();
	if (!ret) {
		printk("swapmon: rdma connection established successfully\n");
	}
	printk("swapmon: module loaded successfully.\n");

	return 0;
}

static void __exit swapmonfs_exit(void) {
	printk("swapmon: module unloaded\n");
}

module_init(swapmonfs_init);
module_exit(swapmonfs_exit);

MODULE_LICENSE("GPL v2");
