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
struct swapmon_rdma_ctrl {
	struct ib_device *dev;

	// protection domain (PD)
	struct ib_pd *pd;
	// queue pair (QP)
	struct ib_qp *qp;
	// completion queue (CQ)
	struct ib_cq *cq;

	enum ib_qp_type qp_type;

	// RDMA identifier created through `rdma_create_id`
	struct rdma_cm_id *cm_id;

	struct completion cm_done;
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

static int swapmon_rdma_parse_ipaddr(struct sockaddr_in *saddr, char *ip) {
	int ret;
  u8 *addr = (u8 *)&saddr->sin_addr.s_addr;
  size_t buflen = strlen(ip);

  pr_info("start: %s\n", __FUNCTION__);

	if (buflen > INET_ADDRSTRLEN) {
    return -EINVAL;
	}
	ret = in4_pton(ip, buflen, addr, '\0', NULL);
  if (ret) {
		printk("in4_pton() failed, unable to parse ip address: %s\n", ip);
    return -EINVAL;
	}
  saddr->sin_family = AF_INET;

  return 0;
}

static void swapmon_rdma_qp_event(struct ib_event *event, void *context) {
	pr_debug("QP event %s (%d)\n", ib_event_msg(event->event), event->event);
}

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


static int swapmon_rdma_addr_resolved() {
	const int send_wr_factor = 3;
	const int cq_factor = send_wr_factor + 1;
	int comp_vector = 0;
	int ret;

	ctrl->dev = ctrl->cm_id->device;
	if (!ctrl->dev) {
		pr_err("No device found.\n");
		ret = PTR_ERR(ctrl->dev);
		goto out_err;
	}

	// allocate a protection domain which we can use to create objects within
	// the domain. we can register and associate memory regions with this PD
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

static int swapmon_rdma_route_resolved() {
	struct rdma_conn_param param = {};
  int ret;

  param.qp_num = ctrl->qp->qp_num;
  param.flow_control = 1;
  param.responder_resources = 16;
  param.initiator_depth = 16;
  param.retry_count = 7;
  param.rnr_retry_count = 7;
  param.private_data = NULL;
  param.private_data_len = 0;

  pr_info("max_qp_rd_atom=%d max_qp_init_rd_atom=%d\n",
      ctrl->dev->attrs.max_qp_rd_atom,
      ctrl->dev->attrs.max_qp_init_rd_atom);

  ret = rdma_connect(ctrl->cm_id, &param);
  if (ret) {
    pr_err("rdma_connect failed (%d)\n", ret);
    ib_free_cq(ctrl->cq);
  }

  return 0;
}

static int swapmon_rdma_cm_event_handler(struct rdma_cm_id *id,
		struct rdma_cm_event *event) {
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
			ctrl->cm_error = 0;
			complete(&ctrl->cm_done);
			break;
		case RDMA_CM_EVENT_ROUTE_ERROR:
		case RDMA_CM_EVENT_ADDR_ERROR:
		case RDMA_CM_EVENT_CONNECT_REQUEST:
		case RDMA_CM_EVENT_CONNECT_RESPONSE:
		case RDMA_CM_EVENT_CONNECT_ERROR:
		case RDMA_CM_EVENT_UNREACHABLE:
		case RDMA_CM_EVENT_REJECTED:
		case RDMA_CM_EVENT_DISCONNECTED:
		case RDMA_CM_EVENT_DEVICE_REMOVAL:
		case RDMA_CM_EVENT_MULTICAST_JOIN:
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


static int swapmon_rdma_wait_for_cm() {
	wait_for_completion_interruptible_timeout(&ctrl->cm_done,
			msecs_to_jiffies(SWAPMON_RDMA_CONNECTION_TIMEOUT_MS) + 1);
	return ctrl->cm_error;
}


static int swapmon_rdma_create_ctrl() {
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

	ret = swapmon_rdma_parse_ipaddr(&(ctrl->client_addr), client_ip);
	if (ret) {
		printk("swapmon: swapmon_rdma_parse_ipaddr() for client ip failed\n");
		return -EINVAL;
	}

	ctrl->cm_id = rdma_create_id(&init_net, swapmon_rdma_cm_event_handler, ctrl,
			RDMA_PS_TCP, ctrl->qp_type);
	if (IS_ERR(ctrl->cm_id)) {
		pr_err("RDMA CM ID creation failed\n");
		return -ENODEV;
	}

	// Resolve destination and optionally source addresses from IP addresses to an
	// RDMA address and if the resolution is successful, then the RDMA cm id will
	// be bound to a local device.
	ret = rdma_resolve_addr(ctrl->cm_id, (struct sockaddr *)&ctrl->client_addr,
			(struct sockaddr *)&ctrl->server_addr, SWAPMON_RDMA_CONNECTION_TIMEOUT_MS);
	if (ret) {
		pr_err("rdma_resolve_addr failed: %d\n", ret);
		goto out_destroy_cm_id;
	}

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

static int rdma_conn_init() {
	int ret;

	// register an IB client, in order to register callbacks for IB device
	// addition and removal. When an IB device is added, each registered
	// client's add method will be called and similarly for removal.
	ret = ib_register_client(&swapmon_ib_client);
	if (ret) {
		printk("swapmon: ib_register_client() failed\n");
		return ret;
	}

	ret = swapmon_rdma_create_ctrl();
	if (ret)
		goto err_unreg_client;

	return 0;

err_unreg_client:
	ib_unregister_client(&swapmon_ib_client);
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
	printk("swapmon: module loaded\n");




	return 0;

}

static void __exit swapmonfs_exit(void) {
	printk("swapmon: module unloaded\n");
}

module_init(swapmonfs_init);
module_exit(swapmonfs_exit);
