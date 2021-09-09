#include <stdio.h>

#include <arpa/inet.h>
#include <rdma/rdma_cma.h>

int on_connect_request(struct rdma_cm_id *cm_id, struct rdma_conn_param *param) {
	int ret;

	struct ibv_pd *pd;
	pd = ibv_alloc_pd(cm_id->verbs);
	if (pd) {
		printf("ibv_alloc_pd() failed.\n");
		return -1;
	}

	struct ibv_qp_init_attr attr;
	memset(&attr, 0, sizeof(struct ibv_qp_init_attr));
	// attr.send_cq = ??
	// attr.recv_cq = ??
	attr.cap.max_send_wr = 1;
	attr.cap.max_recv_wr = 1;
	attr.cap.max_send_sge = 1;
	attr.cap.max_recv_sge = 1;
	attr.cap.max_inline_data = 0;
	attr.qp_type = IBV_QPT_RC;

	ret = rdma_create_qp(cm_id, pd, &attr);
	if (ret) {
		printf("rdma_create_qp() failed with exit code %d.\n", ret);
		return ret;
	}

	struct rdma_conn_param conn_param;
	memset(&conn_param, 0, sizeof(conn_param));
	conn_param.private_data = NULL;
	conn_param.private_data_len = 0;
	conn_param.responder_resources = param->responder_resources;
	conn_param.initiator_depth = param->initiator_depth;
	conn_param.flow_control = param->flow_control;
	conn_param.rnr_retry_count = param->rnr_retry_count;

	ret = rdma_accept(cm_id, &conn_param);
	if (ret) {
		printf("rdma_accept() failed with exit code %d.\n", ret);
		return ret;
	}

	return 0;
}

int cm_event_handler(struct rdma_cm_id *cm_id, struct rdma_cm_event *event) {
	switch (event->event) {
		case RDMA_CM_EVENT_CONNECT_REQUEST:
			// Upon receiving a REQ message, we can allocate a QP and accept the
			// incoming connection request.
			on_connect_request(cm_id, &event->param.conn);
			printf("Received a connection request.\n");
			break;
		case RDMA_CM_EVENT_ESTABLISHED:
			// Upon receiving a message that connection is established, we can
			// acknowledge the event back to the client. The call below also frees
			// the event structure and any memory it references.
			printf("Connection is established successfully.\n");
			break;
	}
}


int main(void) {
	int ret;
	struct rdma_event_channel* ev_channel;
	ev_channel = rdma_create_event_channel();
	if (!ev_channel) {
		printf("Failed to create event channel.\n");
		return -1;
	}

	struct rdma_cm_id *server_cm_id;
	ret = rdma_create_id(ev_channel, &server_cm_id, NULL, RDMA_PS_TCP);
	if (ret) {
		printf("rdma_create_id() failed with exit code %d.\n", ret);
		return ret;
	}

	struct sockaddr_in sa;
	sa.sin_family = AF_INET;
	sa.sin_port = 10000;
	ret = inet_pton(AF_INET, "192.168.1.10", &(sa.sin_addr));
	if (ret <= 0) {
		printf("invalid IP address.\n");
		return ret;
	}

	// bind the RDMA identifier to the source address and port
	ret = rdma_bind_addr(server_cm_id, (struct sockaddr *) &sa);
	if (ret) {
		printf("rdma_bind_addr() failed with exit code %d.\n", ret);
		return ret;
	}

	// Listen for incoming connection requests.
	// The 2nd argument of rdma_listen dictates how many pending rdma
	// connect requests the kernel will keep in-queue for the application
	// to accept/reject.
	ret = rdma_listen(server_cm_id, 2);
	if (ret) {
		printf("rdma_listen() failed with exit code %d.\n", ret);
		return ret;
	}

	printf("Listening in port %d\n", sa.sin_port);

	struct rdma_cm_event *event;
	while (rdma_get_cm_event(ev_channel, &event) == 0) {
		cm_event_handler(server_cm_id, event);
		rdma_ack_cm_event(event);
	}


	rdma_destroy_event_channel(ev_channel);
}
