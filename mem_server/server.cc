#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <infiniband/verbs.h>

#include <cstring>
#include <string>
#include <vector>
#include <iostream>
#include <cerrno>

#define MAX_QUEUED_CONNECT_REQUESTS 2

class ServerRDMA {

 public:
	ServerRDMA(const std::string& ip_addr, int port);
	~ServerRDMA();

	void Listen(const std::string& ip_addr, int port);


 private:
	// Handler of all events communicated through event channel.
	int cm_event_handler(struct rdma_cm_id *ev_cm_id, struct rdma_cm_event *ev);

	// Function to be invoked upon receiving an RDMA connection request (REQ).
	int on_connect_request(struct rdma_cm_id *client_cm_id,
												  struct rdma_conn_param *conn_params);

	// Event channel used to report communication events.
	struct rdma_event_channel *ev_channel_;

	// Communication Manager ID used to track connection communication info.
	struct rdma_cm_id *cm_id_;

	struct ibv_mr *mr_;

	struct ibv_pd *pd_;

	void *buf_;

	// Vector of the RDMA CM ids of the connected clients.
	std::vector<struct rdma_cm_id *> client_cm_ids_;

	// Number of accepted connections.
	int n_conns_;
};


int ServerRDMA::on_connect_request(struct rdma_cm_id *client_cm_id,
		struct rdma_conn_param *param) {
	struct ibv_qp_init_attr attr;
	std::memset(&attr, 0, sizeof(attr));
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


int ServerRDMA::cm_event_handler(struct rdma_cm_id *ev_cm_id,
		struct rdma_cm_event *ev) {
	switch (ev->event) {
		case RDMA_CM_EVENT_CONNECT_REQUEST:
			// Upon receiving a REQ message, we can allocate a QP and accept the
			// incoming connection request.
			std::cout << "Received a connection request.\n";
			on_connect_request(ev->id, &ev->param.conn);
			break;
		case RDMA_CM_EVENT_ESTABLISHED:
			// Upon receiving a message that connection is established, we can
			// acknowledge the event back to the client. The call below also frees
			// the event structure and any memory it references.
			std::cout << "Connection is established successfully.\n";
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


void ServerRDMA::Listen(const std::string& ip_addr, int port) {
	ev_channel_ = rdma_create_event_channel();
	if (ev_channel_ == NULL) {
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
}


ServerRDMA::~ServerRDMA() {
	if (cm_id_ != NULL) {
		// Any associated QP must be freed before destroying the CM ID.
		if (rdma_destroy_id(cm_id_)) {
			std::cerr << "rdma_destroy_id failed: " << std::strerror(errno) << "\n";
		}
	}

	// All rdma_cm_id's associated with the event channel must be destroyed, and
	// all returned events must be acked before destroying the channel.
	if (ev_channel_ != NULL) {
		rdma_destroy_event_channel(ev_channel_);
	}
}

int main() {
	return 0;
}
