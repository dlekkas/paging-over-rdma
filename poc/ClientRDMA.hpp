#pragma once 

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
class ClientRDMA{


public:
 

	static struct rdma_cm_id * sendConnectRequest(char* ip, int port){
 
		struct rdma_cm_id *id = NULL;
		struct rdma_addrinfo *addrinfo = NULL;
		struct rdma_addrinfo hints;

		memset(&hints, 0, sizeof hints);
		hints.ai_port_space = RDMA_PS_TCP;
        
        if (rdma_getaddrinfo(ip, (char*)std::to_string(port).c_str(), &hints, &addrinfo)) {
            perror("rdma_getaddrinfo\n");
            if(addrinfo){
            	rdma_freeaddrinfo(addrinfo);
            }
            return NULL;
        } 
 
        if (rdma_create_ep(&id, addrinfo, NULL, NULL)) {
            perror("rdma_create_ep");
            id = NULL;
        }

        if(addrinfo){
        	rdma_freeaddrinfo(addrinfo);
        }

        return id;
	}

    static struct ibv_qp* create_ud(struct rdma_cm_id * id, struct ibv_pd *pd, struct ibv_qp_init_attr *init_attr){

      struct ibv_qp* qp = ibv_create_qp(pd, init_attr);

      uint8_t port = id->port_num;

      int pkeyindex = 0;

      struct ibv_port_attr port_info = {};
      if (ibv_query_port(pd->context, port, &port_info)) {
        fprintf(stderr, "Unable to query port info for port %d\n", port);
        return 0;
      }
      if(port_info.link_layer == IBV_LINK_LAYER_ETHERNET){
        printf("Link is ethernet. table size %u\n",port_info.pkey_tbl_len);
        pkeyindex = 0;
      } else {
        __be16 pkey = id->route.addr.addr.ibaddr.pkey;
        printf("pkey is %u\n", pkey );
        pkeyindex = ibv_get_pkey_index(pd->context, port, pkey);
      }


      // move QP to INIT state
      struct ibv_qp_attr attr;
      memset(&attr,0,sizeof attr);
      attr.qp_state        = IBV_QPS_INIT;
      attr.pkey_index      = pkeyindex;
      attr.port_num        = port;
      attr.qkey            = 0x11111111; // can be any constant
 
      if (ibv_modify_qp(qp, &attr,
              IBV_QP_STATE              |
              IBV_QP_PKEY_INDEX         |
              IBV_QP_PORT               |
              IBV_QP_QKEY)) {
            fprintf(stderr, "Failed to modify QP to INIT\n");
            return nullptr;
      }
    
      memset(&attr,0,sizeof attr);
      attr.qp_state   = IBV_QPS_RTR;      

      if (ibv_modify_qp(qp, &attr, IBV_QP_STATE)) {
        fprintf(stderr, "Failed to modify QP to RTR\n");
        return nullptr;
      }
 
      memset(&attr,0,sizeof attr);
      attr.qp_state   = IBV_QPS_RTS;
      attr.sq_psn     = 0;

      if (ibv_modify_qp(qp, &attr,
            IBV_QP_STATE              |
            IBV_QP_SQ_PSN)) {
        fprintf(stderr, "Failed to modify QP to RTS\n");
        return nullptr;
      }

      return qp;
    }

 

};