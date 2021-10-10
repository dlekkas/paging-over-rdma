#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netdb.h>


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
#include <chrono>
#include <thread>

#define MAX_QUEUED_CONNECT_REQUESTS 2
#define UD_QKEY 0x11111111
#define IBV_DEFAULT_PKEY_FULL 0xFFFF

// Memory size allocated for remote peers.
constexpr std::size_t BUFFER_SIZE(16 * 4096);

// Completion Queue (CQ) will contain at least `MIN_CQE` entries.
constexpr int MIN_CQE(8192);


enum comm_ctrl_opcode {
	MCAST_MEMBERSHIP_NACK,
	MCAST_MEMBERSHIP_ACK
};


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

	int recv_mcast_grp_info(struct ibv_qp *qp);

	int recv_mcast_dummy_msg();

	void join_mcast_group();

	int send_control_msg(comm_ctrl_opcode op);

	void init_mcast_rdma_resources();

	int extract_pkey_index(uint8_t port_num, __be16 pkey);

	// Event channel used to report communication events.
	struct rdma_event_channel *ev_channel_;

	// Communication Manager ID used to track connection communication info.
	struct rdma_cm_id *cm_id_;

	struct rdma_cm_id *mcast_cm_id_;

	struct ibv_cq *cq_;

	struct ibv_cq *mcast_cq_;

	struct ibv_srq *srq_;

	struct ibv_mr *mr_;

	struct ibv_pd *pd_;

	struct ibv_qp *rc_qp_;

	struct ibv_qp *ud_qp_;

	struct ibv_qp *mcast_qp_;

	struct sockaddr src_addr_;

	char *mcast_msg;

	void *buf_;

	// Vector of the RDMA CM ids of the connected clients.
	std::vector<ConnectionRDMA> clients_;

	// Number of accepted connections.
	int n_conns_;

	struct mcast_grp_info {
		uint8_t gid_raw[16];
		uint16_t lid;
		uint32_t qkey;
	} mcast_info_msg;
};

// Helper function mainly used to ensure that we are not polling forever
// if something goes wrong which causes the kernel driver to crash.
int poll_cq_with_timeout(struct ibv_cq* cq, int num_entries,
		struct ibv_wc *wc, int timeout_ms) {
	const int freq = 100; // ms
	int retries = timeout_ms / freq + 1;

	std::chrono::milliseconds sleep_ms(freq);
	while (retries > 0) {
		int ret = ibv_poll_cq(cq, num_entries, wc);
		if (ret != 0) {
			return ret;
		}
		std::this_thread::sleep_for(sleep_ms);
		retries--;
	}
	return 0;
}

int ServerRDMA::extract_pkey_index(uint8_t port_num, __be16 pkey) {
	struct ibv_port_attr port_info = {};
	if (ibv_query_port(pd_->context, port_num, &port_info)) {
		std::cerr << "ibv_query_port failed: " << std::strerror(errno) << "\n";
		return -1;
	}

	__be16 curr_pkey;
	for (uint16_t i = 0; i < port_info.pkey_tbl_len; i++) {
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

	// Partition Key (P_Key): The partition key provides isolation between nodes
	// and create "virtual fabrics" that only QPs belonging to the same partition
	// can communicate and at least one of those QPs should be a full member of
	// this partition (i.e. having MSB = 1).
	/*
	__be16 pkey = cm_id_->route.addr.addr.ibaddr.pkey;
	attr.pkey_index = extract_pkey_index(cm_id_->port_num, pkey);
	if (attr.pkey_index < 0) {
		std::cerr << "extract_pkey_index(" << +cm_id_->port_num << ", "
							<< pkey << ") failed.\n";
		// try out the default full membership P_Key (0xFFFF) because each P_Key
		// table has at least this valid key
		attr.pkey_index = extract_pkey_index(cm_id_->port_num,
				IBV_DEFAULT_PKEY_FULL);
		if (attr.pkey_index < 0) {
			std::cerr << "extract_pkey_index(" << +cm_id_->port_num << ", "
								<< pkey << ") failed.\n";
			return -2;
		}
	}
	*/
	/*
	struct ibv_qp_attr uc_qp_attr;
	struct ibv_qp_init_attr tmp_attr;
	if(ibv_query_qp(cm_id_->qp, &uc_qp_attr, IBV_QP_PKEY_INDEX, &tmp_attr)) {
		std::cerr << "ibv_query_qp failed: " << std::strerror(errno) << "\n";
	}
	// attr.pkey_index = uc_qp_attr.pkey_index;
	if (cm_id_->qp == nullptr) {
		std::cerr << "cm_id_->qp is NULL.\n";
	}
	*/
	attr.pkey_index = 0;

	attr.port_num = cm_id_->port_num;

	std::cout << "qpn = " << ud_qp_->qp_num << ", pkey_index = " <<
							 attr.pkey_index << ", port_num = " << +attr.port_num << "\n";

	__be16 pkey;
	if (ibv_query_pkey(pd_->context, attr.port_num, attr.pkey_index, &pkey)) {
		std::cerr << "ibv_query_pkey failed: " << std::strerror(errno) << "\n";
	}

	// Queue Key (Q_Key): An Unreliable Datagram (UD) queue pair will get unicast
	// or multicast messages from a remote UD QP only if the Q_key of the message
	// is equal to the Q_key value of this UD QP. The remote peer that will send
	// msg to this QP should specify the same Q_key as below.
	attr.qkey = UD_QKEY;
	std::cout << "pkey = " << pkey << ", qkey = " << attr.qkey << "\n";

	if (ibv_modify_qp(ud_qp_, &attr,
				IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_QKEY)) {
		std::cerr << "ibv_modify_qp failed to move QP to state INIT.\n";
		std::cerr << "ibv_modify_qp failed: " << std::strerror(errno) << "\n";
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
	rc_qp_ = client_cm_id->qp;

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

int ServerRDMA::send_control_msg(comm_ctrl_opcode op) {
	struct ibv_send_wr wr, *bad_wr;
	wr.wr_id = 1000;
	wr.next = NULL;

	wr.sg_list = NULL;
	wr.num_sge = 0;
	wr.opcode = IBV_WR_SEND_WITH_IMM;
	wr.send_flags = IBV_SEND_SIGNALED;
	wr.imm_data = htonl(op);

	if (ibv_post_send(rc_qp_, &wr, &bad_wr)) {
		std::cerr << "ibv_post_send() failed: " << std::strerror(errno) << "\n";
		return -1;
	}

	struct ibv_wc wc;
	int timeout_ms = 10000;
	int ret = poll_cq_with_timeout(rc_qp_->send_cq, 1, &wc, timeout_ms);

	if (ret <= 0) {
		std::cout << "Failure when sending control message.\n";
		return ret;
	}

	if (wc.status != IBV_WC_SUCCESS) {
		std::cerr << "Polling failed with status " << ibv_wc_status_str(wc.status)
							<< ", work request ID: " << wc.wr_id << std::endl;
		return -1;
	}
	return 0;
}

int ServerRDMA::recv_mcast_dummy_msg() {
	struct ibv_sge sg;
	struct ibv_recv_wr wr;
	struct ibv_recv_wr *bad_wr;
	std::size_t msg_len = sizeof "Hello mcast world!";
	std::size_t total_len = msg_len + sizeof(struct ibv_grh);

	mcast_msg = reinterpret_cast<char*>(malloc(total_len));

	struct ibv_mr *recv_mr;
	recv_mr = ibv_reg_mr(pd_, mcast_msg, total_len,
			IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
	if (recv_mr == nullptr) {
		std::cerr << "ibv_reg_mr failed.\n";
	}

	sg.addr = reinterpret_cast<uint64_t>(mcast_msg);
	sg.length = total_len;
	sg.lkey = recv_mr->lkey;

	wr.wr_id = 420;
	wr.next = NULL;
	wr.sg_list = &sg;
	wr.num_sge = 1;

	if (ibv_post_recv(mcast_qp_, &wr, &bad_wr)) {
		std::cerr << "ibv_post_recv() failed.\n";
		return -1;
	}

	std::cout << "recv_mcast_dummy_msg() completed successfully.\n";
	return 0;
}

int ServerRDMA::recv_mcast_grp_info(struct ibv_qp *qp) {
	struct ibv_sge sg;
	struct ibv_recv_wr wr;
	struct ibv_recv_wr *bad_wr;

	struct ibv_mr *recv_mr;
	recv_mr = ibv_reg_mr(pd_, &mcast_info_msg, sizeof(mcast_info_msg),
			IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
	if (recv_mr == nullptr) {
		std::cerr << "ibv_reg_mr failed.\n";
	}

	sg.addr = reinterpret_cast<uint64_t>(&mcast_info_msg);
	sg.length = sizeof(mcast_info_msg);
	sg.lkey = recv_mr->lkey;

	wr.wr_id = 200;
	wr.next = NULL;
	wr.sg_list = &sg;
	wr.num_sge = 1;

	if (ibv_post_recv(qp, &wr, &bad_wr)) {
		std::cerr << "ibv_post_recv() failed.\n";
		return -1;
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
						<< ", key = " << server_info_msg.key
						<< ", qpn = " << server_info_msg.qpn << "\n";

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

			if (recv_mcast_grp_info(ev_cm_id->qp)) {
				std::cout << "Failed to post RR for multicast group info.\n";
			}

			if (send_mem_info(ev_cm_id->qp)) {
				std::cout << "Failed to exchange memory region info.\n";
			} else {
				std::cout << "Memory region info sent successfully.\n";
			}

			struct ibv_wc wc;
			while (!ibv_poll_cq(ev_cm_id->qp->recv_cq, 1, &wc)) {
				// nothing
			}

			if (wc.status != IBV_WC_SUCCESS) {
				std::cerr << "Polling failed with status " << ibv_wc_status_str(wc.status)
									<< ", work request ID: " << wc.wr_id << std::endl;
				return -1;
			}

			std::cout << "Received message with wr_id = " << wc.wr_id << "\n";
			std::cout << "lid = " << mcast_info_msg.lid << "\n";
			std::cout << "qkey = " << mcast_info_msg.qkey << "\n";

			join_mcast_group();
			recv_mcast_dummy_msg();

			clients_.emplace_back(ev_cm_id, ev_cm_id->qp, ev_cm_id->qp->pd);
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
			break;
		case RDMA_CM_EVENT_MULTICAST_JOIN:
			std::cout << "Joined multicast group successfully.\n";
			union ibv_gid mgid;
			std::memcpy(mgid.raw, mcast_info_msg.gid_raw, 16);
			if (ibv_attach_mcast(mcast_qp_, &mgid, mcast_info_msg.lid)) {
				std::cerr << "ibv_attach_mcast() failed.\n";
			} else {
				std::cout << "Attached to mcast group successfully.\n";
			}

			send_control_msg(MCAST_MEMBERSHIP_ACK);

			n_conns_++;
			break;
		case RDMA_CM_EVENT_MULTICAST_ERROR:
		case RDMA_CM_EVENT_ADDR_CHANGE:
		case RDMA_CM_EVENT_TIMEWAIT_EXIT:
		default:
			std::cerr << "Unrecognized RDMA CM event " << ev->event << "\n";
			break;
	}
	return 0;
}

void ServerRDMA::init_mcast_rdma_resources() {
	/*
	if (rdma_create_id(ev_channel_, &mcast_cm_id_, NULL, RDMA_PS_UDP)) {
		std::cerr << "rdma_create_id() failed: " << std::strerror(errno) << "\n";
		return;
	}
	*/

	if (!pd_) {
		std::cerr << "Protection domain not initialized.\n";
		return;
	}

	mcast_cq_ = ibv_create_cq(pd_->context, MIN_CQE, NULL, NULL, 0);
	if (!cq_) {
		std::cerr << "ibv_create_cq failed: " << std::strerror(errno) << "\n";
		return;
		// std::exit(EXIT_FAILURE);
	}

	/*
	std::string ip("192.168.1.20");
	struct rdma_addrinfo hints;
	hints.ai_port_space = RDMA_PS_UDP;
	hints.ai_flags = RAI_PASSIVE;

	struct rdma_addrinfo *res;
	std::cout << "getaddrinfo()\n";
	if (rdma_getaddrinfo(ip.c_str(), NULL, &hints, &res)) {
		std::cerr << "rdma_getaddrinfo() failed: " << std::strerror(errno) << "\n";
	}
	*/

	/*
	struct sockaddr_in sa;
	inet_pton(AF_INET, "192.168.1.20", &(sa.sin_addr));

	std::cout << "bindaddr()\n";
	if (rdma_bind_addr(mcast_cm_id_, (struct sockaddr *) &sa)) {
		std::cerr << "rdma_bind_addr() failed: " << std::strerror(errno) << "\n";
	}
	std::cout << "done\n";
	*/

	struct ibv_qp_init_attr attr;
	std::memset(&attr, 0, sizeof(attr));
	attr.send_cq = mcast_cq_;
	attr.recv_cq = mcast_cq_;
	attr.srq = NULL;
	attr.cap.max_send_wr = 6;
	attr.cap.max_recv_wr = 6;
	attr.cap.max_send_sge = 1;
	attr.cap.max_recv_sge = 1;
	attr.cap.max_inline_data = 16;
	attr.qp_type = IBV_QPT_UD;
	attr.sq_sig_all = 0;
	if (rdma_create_qp(mcast_cm_id_, pd_, &attr)) {
		std::cerr << "[2]rdma_create_qp() failed: " << std::strerror(errno) << "\n";
	}

	mcast_qp_ = mcast_cm_id_->qp;
}

void ServerRDMA::join_mcast_group() {
	init_mcast_rdma_resources();

	std::cout << "init_mcast_rdma_resources() finished successfully.\n";

	char addr_buf[INET6_ADDRSTRLEN];
	if (!inet_ntop(AF_INET6, mcast_info_msg.gid_raw,
				addr_buf, INET6_ADDRSTRLEN)) {
		std::cerr << "inet_ntop failed to convert multicast address.\n";
	}

	std::string mcast_addr_str(addr_buf);
	std::cout << "mcast grp addr = " << mcast_addr_str << "\n";

	struct rdma_addrinfo hints;
	std::memset(&hints, 0, sizeof(hints));
	hints.ai_port_space = RDMA_PS_UDP;
	hints.ai_flags = 0;

	struct rdma_addrinfo *addrinfo;
	if (rdma_getaddrinfo(mcast_addr_str.c_str(), NULL, &hints, &addrinfo)) {
		std::cerr << "rdma_getaddrinfo() failed: " << std::strerror(errno) << "\n";
	}

	struct rdma_cm_join_mc_attr_ex mc_join_attr;
	mc_join_attr.comp_mask = RDMA_CM_JOIN_MC_ATTR_ADDRESS |
													 RDMA_CM_JOIN_MC_ATTR_JOIN_FLAGS;
	mc_join_attr.addr = addrinfo->ai_dst_addr;
	mc_join_attr.join_flags = RDMA_MC_JOIN_FLAG_FULLMEMBER;
	if (rdma_join_multicast_ex(mcast_cm_id_, &mc_join_attr, NULL)) {
		std::cerr << "rdma_join_multicast_ex() failed: "
			<< std::strerror(errno) << "\n";
	}
	std::cout << "rdma_join_multicast_ex.\n";
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

	if (rdma_create_id(ev_channel_, &mcast_cm_id_, NULL, RDMA_PS_UDP)) {
		std::cerr << "rdma_create_id() failed: " << std::strerror(errno) << "\n";
		return;
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

	// Bind the RDMA CM identifier to the source address and port.
	if (rdma_bind_addr(mcast_cm_id_, res->ai_src_addr)) {
		std::cerr << "[2]rdma_bind_addr() failed: " << std::strerror(errno) << "\n";
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

	std::cout << "Polling for mcast dummy msg (wait up to 10s).\n";

	struct ibv_wc wc;
	int timeout_ms = 10000;
	int ret = poll_cq_with_timeout(mcast_cq_, 1, &wc, timeout_ms);

	if (ret <= 0) {
		std::cout << "No multicast message received.\n";
	} else {
		if (wc.status != IBV_WC_SUCCESS) {
			std::cerr << "Polling failed with status " << ibv_wc_status_str(wc.status)
								<< ", work request ID: " << wc.wr_id << std::endl;
			return -1;
		}
		std::cout << "Received mcast message with wr_id = " << wc.wr_id << "\n";
		std::cout << "Message = " << &mcast_msg[sizeof(struct ibv_grh)] << std::endl;
	}

	/*
	struct ibv_wc wc;
	while (!ibv_poll_cq(mcast_cq_, 1, &wc)) {
		// nothing
	}
	*/

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

	std::cout << "Pausing for 20sec." << std::endl;
	std::chrono::milliseconds pause_tm(20000);
	std::this_thread::sleep_for(pause_tm);

	return 0;
}
