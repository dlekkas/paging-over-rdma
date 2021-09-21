#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <infiniband/verbs.h>

#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <iostream>
#include <cerrno>

#define MAX_QUEUED_CONNECT_REQUESTS 2
#define UDP_QKEY 0x11111111
#define IBV_DEFAULT_PKEY_FULL 0xFFFF

// Memory size allocated for remote peers.
constexpr std::size_t BUFFER_SIZE(16 * 4096);

// Completion Queue (CQ) will contain at least `MIN_CQE` entries.
constexpr int MIN_CQE(8192);


class ConnectionRDMA {
 public:
	struct rdma_cm_id* id;
	struct ibv_qp* qp;

	// Ownernship of protection domain is on ServerRDMA (i.e. do not destroy here)
	struct ibv_pd* pd;

	ConnectionRDMA(struct rdma_cm_id *id, struct ibv_qp *qp, struct ibv_pd* pd)
		: id(id), qp(qp), pd(pd) {}

	~ConnectionRDMA() {
		if (qp != nullptr) {
			if (ibv_destroy_qp(qp)) {
				std::cerr << "ibv_destroy_qp failed: " << std::strerror(errno) << "\n";
			}
		}

		if (id != nullptr) {
			// Any associated QP must be freed before destroying the CM ID.
			if (rdma_destroy_id(id)) {
				std::cerr << "rdma_destroy_id failed: " << std::strerror(errno) << "\n";
			}
		}
	}
};


class ServerRDMA {

 public:
	ServerRDMA(): n_conns_(0) {}

	~ServerRDMA();

	void Listen(const std::string& ip_addr, int port);

	int WaitForClients(int n_clients);

	int InitUDP();


 private:
	// Handler of all events communicated through event channel.
	int cm_event_handler(struct rdma_cm_event *ev);

	// Function to be invoked upon receiving an RDMA connection request (REQ).
	int on_connect_request(struct rdma_cm_id *client_cm_id,
												 struct rdma_conn_param *conn_params);

	int setup_mem_region(std::size_t buf_size);

	// Send memory info to the queue pair specified as argument to inform the
	// client about the memory address, length and lkey.
	int send_mem_info(struct ibv_qp *qp);

	int extract_pkey_index(uint8_t port_num, __be16 pkey);

	// Event channel used to report communication events.
	struct rdma_event_channel *ev_channel_;

	// Communication Manager ID used to track connection communication info.
	struct rdma_cm_id *cm_id_;

	struct ibv_cq *cq_;

	struct ibv_srq *srq_;

	struct ibv_mr *mr_;

	struct ibv_pd *pd_;

	struct ibv_qp *ud_qp_;

	void *buf_;

	// Vector of the RDMA CM ids of the connected clients.
	std::vector<ConnectionRDMA> clients_;

	// Number of accepted connections.
	int n_conns_;
};

int ServerRDMA::extract_pkey_index(uint8_t port_num, __be16 pkey) {
	struct ibv_port_attr port_info = {};
	if (ibv_query_port(pd_->context, port_num, &port_info)) {
		std::cerr << "ibv_query_port failed: " << std::strerror(errno) << "\n";
		return -1;
	}

	for (uint16_t i = 0; i < port_info.pkey_tbl_len; i++) {
		uint16_t curr_pkey;
		if (ibv_query_pkey(pd_->context, port_num, i, &curr_pkey) == 1 &&
				curr_pkey == pkey) {
			return i;
		}
	}

	return -1;
}

int ServerRDMA::InitUDP() {
	// TODO(dimlek): tune the attributes below
	struct ibv_qp_init_attr init_attr;
	std::memset(&init_attr, 0, sizeof(init_attr));
	init_attr.send_cq = cq_;
	init_attr.recv_cq = cq_;
	init_attr.srq = NULL;
	init_attr.qp_type = IBV_QPT_UD;

	// We only receive UC messages, but we reply only with RC.
	init_attr.cap.max_send_wr = 0;
	init_attr.cap.max_recv_wr = 8192;
	init_attr.cap.max_send_sge = 0;
	init_attr.cap.max_recv_sge = 2;

	ud_qp_ = ibv_create_qp(pd_, &init_attr);
	if (!ud_qp_) {
		std::cerr << "ibv_create_qp for UD failed: "
							<< std::strerror(errno) << "\n";
		return -1;
	}

	struct ibv_qp_attr attr;
	std::memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_INIT;
	attr.port_num = cm_id_->port_num;

	// Partition Key (P_Key): The partition key provides isolation between nodes
	// and create "virtual fabrics" that only QPs belonging to the same partition
	// can communicate and at least one of those QPs should be a full member of
	// this partition (i.e. having MSB = 1).
	__be16 pkey = cm_id_->route.addr.addr.ibaddr.pkey;
	attr.pkey_index = extract_pkey_index(cm_id_->port_num, pkey);
	if (attr.pkey_index < 0) {
		std::cerr << "extract_pkey_index(" << cm_id_->port_num << ", "
							<< pkey << ") failed.\n";
		// try out the default full membership P_Key (0xFFFF) because each P_Key
		// table has at least this valid key
		attr.pkey_index = extract_pkey_index(cm_id_->port_num,
				IBV_DEFAULT_PKEY_FULL);
		if (attr.pkey_index < 0) {
			std::cerr << "extract_pkey_index(" << cm_id_->port_num << ", "
								<< pkey << ") failed.\n";
			return -2;
		}
	}

	std::cout << "qpn = " << ud_qp_->qp_num << ", pkey_index = " <<
							 attr.pkey_index << ", port_num = " << attr.port_num << "\n";

	// Queue Key (Q_Key): An Unreliable Datagram (UD) queue pair will get unicast
	// or multicast messages from a remote UD QP only if the Q_key of the message
	// is equal to the Q_key value of this UD QP. The remote peer that will send
	// msg to this QP should specify the same Q_key as below.
	attr.qkey = UDP_QKEY;

	if (ibv_modify_qp(ud_qp_, &attr,
				IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_QKEY)) {
		std::cerr << "ibv_modify_qp failed to move QP to state INIT.\n";
		return -3;
	}

	std::memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_RTR;

	if (ibv_modify_qp(ud_qp_, &attr, IBV_QP_STATE)) {
		std::cerr << "ibv_modify_qp failed to move QP to state RTR.\n";
		return -3;
	}

	return 0;
}


int ServerRDMA::on_connect_request(struct rdma_cm_id *client_cm_id,
		struct rdma_conn_param *param) {
	struct ibv_qp_init_attr attr;
	std::memset(&attr, 0, sizeof(attr));
	attr.send_cq = cq_;
	attr.recv_cq = cq_;
	attr.srq = NULL;
	attr.cap.max_send_wr = 6;
	attr.cap.max_recv_wr = 6;
	attr.cap.max_send_sge = 1;
	attr.cap.max_recv_sge = 1;
	attr.cap.max_inline_data = 16;
	attr.qp_type = IBV_QPT_RC;
	attr.sq_sig_all = 0;

	if (rdma_create_qp(client_cm_id, pd_, &attr)) {
		std::cerr << "rdma_create_qp() failed: " << std::strerror(errno) << "\n";
		return 1;
	}

	struct rdma_conn_param conn_param;
	std::memset(&conn_param, 0, sizeof(conn_param));
	conn_param.private_data = NULL;
	conn_param.private_data_len = 0;
	conn_param.responder_resources = param->responder_resources;
	conn_param.initiator_depth = param->initiator_depth;
	conn_param.flow_control = param->flow_control;
	conn_param.rnr_retry_count = param->rnr_retry_count;

	if (rdma_accept(client_cm_id, &conn_param)) {
		std::cerr << "rdma_accept() failed: " << std::strerror(errno) << "\n";
		return 1;
	}

	return 0;
}

int ServerRDMA::send_mem_info(struct ibv_qp *qp) {

	std::cout << "Invoking InitUDP()\n";
	InitUDP();
	std::cout << "InitUDP() completed.\n";

	struct {
		uint64_t addr;
		uint32_t len;
		uint32_t key;
		uint32_t qpn;
	} server_info_msg;

	server_info_msg.addr = reinterpret_cast<uint64_t>(buf_);
	server_info_msg.len = BUFFER_SIZE;
	server_info_msg.key = mr_->rkey;
	server_info_msg.qpn = ud_qp_->qp_num;

	std::cout << "Sending memory info: addr = " << server_info_msg.addr
						<< ", len = " << server_info_msg.len
						<< ", key = " << server_info_msg.key << "\n";

	struct ibv_sge sge;
	sge.addr = reinterpret_cast<uint64_t>(&server_info_msg);
	sge.length = sizeof(server_info_msg);
	sge.lkey = mr_->lkey;

	struct ibv_send_wr *bad_wr;
	struct ibv_send_wr wr;
	wr.wr_id = 0;
	wr.next = NULL;
	wr.sg_list = &sge;
	wr.num_sge = 1;
	wr.opcode = IBV_WR_SEND;
	wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;

	if (ibv_post_send(qp, &wr, &bad_wr)) {
		std::cerr << "ibv_post_send() failed: " << std::strerror(errno) << "\n";
		return -1;
	}

	struct ibv_wc wc;
	while (!ibv_poll_cq(qp->send_cq, 1, &wc)) {
		// nothing
	}

	if (wc.status != IBV_WC_SUCCESS) {
		std::cerr << "Polling failed with status " << ibv_wc_status_str(wc.status)
							<< ", work request ID: " << wc.wr_id << std::endl;
		return -1;
	}

	return 0;
}


int ServerRDMA::cm_event_handler(struct rdma_cm_event *ev) {
	struct rdma_cm_id *ev_cm_id = ev->id;
	switch (ev->event) {
		case RDMA_CM_EVENT_CONNECT_REQUEST:
			// Upon receiving a REQ message, we can allocate a QP and accept the
			// incoming connection request.
			on_connect_request(ev_cm_id, &ev->param.conn);
			break;
		case RDMA_CM_EVENT_ESTABLISHED:
			// Upon receiving a message that connection is established, we can
			// acknowledge the event back to the client. The call below also frees
			// the event structure and any memory it references.
			std::cout << "Connection established.\n";
			if (send_mem_info(ev_cm_id->qp)) {
				std::cout << "Failed to exchange memory region info.\n";
			} else {
				std::cout << "Memory region info sent successfully.\n";
			}
			clients_.emplace_back(ev_cm_id, ev_cm_id->qp, ev_cm_id->qp->pd);
			n_conns_++;
			break;
		case RDMA_CM_EVENT_ADDR_RESOLVED:
		case RDMA_CM_EVENT_ADDR_ERROR:
		case RDMA_CM_EVENT_ROUTE_RESOLVED:
		case RDMA_CM_EVENT_ROUTE_ERROR:
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
		default:
			std::cerr << "Unrecognized RDMA CM event " << ev->event << "\n";
			break;
	}
	return 0;
}

int ServerRDMA::setup_mem_region(std::size_t buf_size) {
	pd_ = ibv_alloc_pd(cm_id_->verbs);
	if (pd_ == nullptr) {
		std::cerr << "ibv_alloc_pd() failed: " << std::strerror(errno) << "\n";
		return -1;
	}

	// ensure page aligned memory for better performance
	std::size_t page_sz = sysconf(_SC_PAGESIZE);
	if (BUFFER_SIZE % page_sz != 0) {
		std::cerr << "Total mem size not a multiple of page size.\n";
	}

	buf_ = aligned_alloc(page_sz, BUFFER_SIZE);
	if (buf_ == nullptr) {
		std::cerr << "Failed to allocate " << BUFFER_SIZE << " bytes.\n";
		return -1;
	}

	// By registering our buffer's address, we allow the RDMA device to read/write
	// data in that region. This call basically pins memory to inform the kernel
	// that the registered memory is for RDMA communication for the application.
	mr_ = ibv_reg_mr(pd_, buf_, BUFFER_SIZE, IBV_ACCESS_LOCAL_WRITE |
																					 IBV_ACCESS_REMOTE_WRITE |
																					 IBV_ACCESS_REMOTE_READ);
	if (mr_ == nullptr) {
		std::cerr << "ibv_reg_mr() failed: " << std::strerror(errno) << "\n";
		return -1;
	}
	return 0;
}


void ServerRDMA::Listen(const std::string& ip_addr, int port) {
	ev_channel_ = rdma_create_event_channel();
	if (ev_channel_ == nullptr) {
		std::cerr << "rdma_create_event_channel() failed: "
							<< std::strerror(errno) << "\n";
		std::exit(EXIT_FAILURE);
	}

	if (rdma_create_id(ev_channel_, &cm_id_, NULL, RDMA_PS_TCP)) {
		std::cerr << "rdma_create_id() failed: " << std::strerror(errno) << "\n";
		std::exit(EXIT_FAILURE);
	}

	struct rdma_addrinfo hints;
	std::memset(&hints, 0, sizeof(hints));
	hints.ai_port_space = RDMA_PS_TCP;
	hints.ai_flags = RAI_PASSIVE;

	struct rdma_addrinfo *res;
	if (rdma_getaddrinfo(ip_addr.c_str(), std::to_string(port).c_str(),
											 &hints, &res)) {
		std::cerr << "rdma_getaddrinfo() failed: " << std::strerror(errno) << "\n";
		std::exit(EXIT_FAILURE);
	}

	// Bind the RDMA CM identifier to the source address and port.
	if (rdma_bind_addr(cm_id_, res->ai_src_addr)) {
		std::cerr << "rdma_bind_addr() failed: " << std::strerror(errno) << "\n";
		std::exit(EXIT_FAILURE);
	}

	// Start listening for incoming connection requests while allowing for a
	// maximum of MAX_QUEUED_CONNECT_REQUESTS to be kept in kernel queue for
	// the application to accept or reject.
	if (rdma_listen(cm_id_, MAX_QUEUED_CONNECT_REQUESTS)) {
		std::cerr << "rdma_listen() failed: " << std::strerror(errno) << "\n";
		std::exit(EXIT_FAILURE);
	}

	int listen_port = ntohs(rdma_get_src_port(cm_id_));
	std::cout << "Listening in port " << std::to_string(listen_port) << "\n";

	if (setup_mem_region(BUFFER_SIZE)) {
		std::cerr << "Setting up memory failed.\n";
		std::exit(EXIT_FAILURE);
	}

	// TODO(dimlek): think about init values
	struct ibv_srq_init_attr srq_init_attr = {};
	// Max number of outstanding work requests in the SRQ.
	srq_init_attr.attr.max_wr = 16;
	// Max number of scatter/gather entries per work request (WR). Here we only
	// want to allow empty messages.
	srq_init_attr.attr.max_sge = 0;
	srq_ = ibv_create_srq(pd_, &srq_init_attr);
	if (!srq_) {
		std::cerr << "ibv_create_srq failed: " << std::strerror(errno) << "\n";
		std::exit(EXIT_FAILURE);
	}

	cq_ = ibv_create_cq(pd_->context, MIN_CQE, NULL, NULL, 0);
	if (!cq_) {
		std::cerr << "ibv_create_cq failed: " << std::strerror(errno) << "\n";
		std::exit(EXIT_FAILURE);
	}
}

int ServerRDMA::WaitForClients(int n_clients) {
	// Retrieve communication events until `n_clients` have successfully
	// established their connection with our server.
	struct rdma_cm_event *event;
	while (n_conns_ < n_clients && !rdma_get_cm_event(ev_channel_, &event)) {
		cm_event_handler(event);
		rdma_ack_cm_event(event);
	}

	if (n_conns_ < n_clients) {
		std::cerr << "rmda_get_cm_event() failed: " << std::strerror(errno) << "\n";
	}

	return n_conns_;
}


ServerRDMA::~ServerRDMA() {
	if (mr_ != nullptr) {
		if (ibv_dereg_mr(mr_)) {
			std::cerr << "ibv_dereg_mr failed: " << std::strerror(errno) << "\n";
		}
	}

	// A PD cannot be destroyed if any QP, region, or AH is still a member of it.
	if (pd_ != nullptr) {
		if (ibv_dealloc_pd(pd_)) {
			std::cerr << "ibv_dealloc_pd failed: " << std::strerror(errno) << "\n";
		}
	}

	if (cm_id_ != nullptr) {
		// Any associated QP must be freed before destroying the CM ID.
		if (rdma_destroy_id(cm_id_)) {
			std::cerr << "rdma_destroy_id failed: " << std::strerror(errno) << "\n";
		}
	}

	// All rdma_cm_id's associated with the event channel must be destroyed, and
	// all returned events must be acked before destroying the channel.
	if (ev_channel_ != nullptr) {
		rdma_destroy_event_channel(ev_channel_);
	}
}

int main(int argc, char *argv[]) {
	if (argc != 3) {
		std::cout << "Usage: " << argv[0] << " <ip> <port>" << std::endl;
		std::exit(2);
	}

	std::size_t pos;
	int port = std::stoi(argv[2], &pos);
	std::string ip_addr = argv[1];

	ServerRDMA server;
	server.Listen(ip_addr, port);

	int connected_clients = server.WaitForClients(1);
	std::cout << "Num of clients connected: " << connected_clients << std::endl;

	return 0;
}
