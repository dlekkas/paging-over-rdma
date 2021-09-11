#pragma once 

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
class ServerRDMA{

    struct rdma_event_channel * _ec;
    struct rdma_cm_id* _listen_id;
    struct ibv_pd* _pd;


    std::vector<union ibv_gid > valid_gids;
    uint32_t udqpn;

    void* buffer;
    struct ibv_mr *mr;

public:
    ServerRDMA(){ // fake server
      _pd = NULL;
    }

    ServerRDMA(char* ip, int port){

        this->_ec = rdma_create_event_channel();
        if(!this->_ec){
            printf("failed to create event channel\n");
            exit(1);
        }
 
        if(rdma_create_id(this->_ec, &this->_listen_id, NULL, RDMA_PS_TCP))
        {
            printf("failed to create listen id\n");
            exit(1);
        }
        rdma_addrinfo *addrinfo;
        rdma_addrinfo hints;

        memset(&hints, 0, sizeof(hints));
        hints.ai_port_space = RDMA_PS_TCP;
        hints.ai_flags = RAI_PASSIVE;

        if(rdma_getaddrinfo(ip, (char*)std::to_string(port).c_str(), &hints, &addrinfo))
        {
            printf("failed to get addr info\n");
            exit(1);
        }

        if(rdma_bind_addr(this->_listen_id,  addrinfo->ai_src_addr)){
            printf("failed to bind addr\n");
            exit(1);
        }


    // Start listening
  
        if(rdma_listen(this->_listen_id, 0)){
            printf("failed to start listening\n");
            exit(1);
        }
    
 
        // Alocate protection domain
        if(!_listen_id->pd){
          _pd = _listen_id->pd;
        } else {
          _pd = ibv_alloc_pd(_listen_id->verbs);
        }
        buffer = malloc(256);
        mr = ibv_reg_mr(_pd, buffer, 256,  0); 
    }  

    struct ibv_pd* getPD() const
    {
        return this->_pd;
    }

    uint8_t get_ibport() const 
    {
        return _listen_id->port_num;
    }

    uint32_t get_mtu(){
      struct ibv_port_attr port_info = {};
      if (ibv_query_port(_pd->context, get_ibport(), &port_info)) {
        fprintf(stderr, "Unable to query port info for port %d\n", get_ibport());
        return 1;
      }
      int mtu = 1 << (port_info.active_mtu + 7); // MTU is usually a power of 2, so the port info only stores the exponent - 7 (you can find it in man of verbs. )
      printf("The Maximum payload size is %d\n",mtu);
      printf("The port lid is 0 for roce. Let's check: %d\n",port_info.lid); // LID (local port identifier) is 0 for roce
      return (uint32_t)mtu; 
    }
    
    int get_pkeyindex() const 
    {

      struct ibv_port_attr port_info = {};
      if (ibv_query_port(_pd->context, get_ibport(), &port_info)) {
        fprintf(stderr, "Unable to query port info for port %d\n", get_ibport());
        return 0;
      }
      if(port_info.link_layer == IBV_LINK_LAYER_ETHERNET){
        printf("Link is ethernet. table size %u\n",port_info.pkey_tbl_len);
        return 0;
      }

      __be16 pkey = _listen_id->route.addr.addr.ibaddr.pkey;
      printf("pkey is %u\n", pkey );
      int index = ibv_get_pkey_index(_pd->context, get_ibport(), pkey);
      printf("pkeyindex is %u\n", index);
      return index;
    }

    static int null_gid(union ibv_gid *gid)
    {
      return !(gid->raw[8] | gid->raw[9] | gid->raw[10] | gid->raw[11] |
         gid->raw[12] | gid->raw[13] | gid->raw[14] | gid->raw[15]);
    }

    void query_all_gids()  
    {

      struct ibv_port_attr port_info = {};
      if (ibv_query_port(_pd->context, get_ibport(), &port_info)) {
        fprintf(stderr, "Unable to query port info for port %d\n", get_ibport());
      }

      for(int i=0; i < port_info.gid_tbl_len; i++){

        union ibv_gid   gid; // address of the destination.
     
        if(ibv_query_gid(_pd->context, get_ibport(), i, &gid)){
            fprintf(stderr, "Failed to query GID[%d]\n",i);
            continue;
        }
        if (!null_gid(&gid)){
            valid_gids.push_back(gid);
            printf("GID[%d].  subnet %llu\n\t inteface %llu\n",i,gid.global.subnet_prefix, gid.global.interface_id);
        
        }
      }
    }


    struct ibv_qp* create_ud_qp(struct ibv_qp_init_attr *init_attr)
    {
      struct ibv_qp* qp = ibv_create_qp(_pd, init_attr);
      // move QP to INIT state
      struct ibv_qp_attr attr;
      memset(&attr,0,sizeof attr);
      attr.qp_state        = IBV_QPS_INIT;
      attr.pkey_index      = get_pkeyindex();
      attr.port_num        = get_ibport();
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

      udqpn = qp->qp_num;
      return qp;
    }


    void send_gid_info(struct rdma_cm_id* id){
                     // we can send info to it.
                      struct ibv_send_wr wr, *bad;
                      printf("Sending %lu gids\n",valid_gids.size());
                      uint32_t len = 8 + valid_gids.size()*sizeof(union ibv_gid);
                      struct ibv_sge sge = {(uint64_t)mr->addr, len, mr->lkey};
                      *((uint32_t*)buffer) = udqpn;
                      *(((uint32_t*)buffer)+1) = valid_gids.size();
                      union ibv_gid* gids = (union ibv_gid*)(((uint32_t*)buffer)+2);
                      for(uint32_t i=0; i<valid_gids.size(); i++){
                        gids[i] = valid_gids[i];
                      }

                      wr.wr_id = 0;
                      wr.next = NULL;
                      wr.sg_list = &sge;
                      wr.num_sge = 1;
                      wr.opcode = IBV_WR_SEND;
                      wr.send_flags = IBV_SEND_SIGNALED;    // todo optimize. Signal sometimes.

                      ibv_post_send(id->qp, &wr, &bad);  
    }

 

    std::pair<struct rdma_cm_id*, void*> get_connect_request( )
    {

        int has_pending = 0;
        rdma_cm_event* event;
        struct rdma_cm_id* id = NULL;
        void* connect_buffer = NULL;

        while(!has_pending){
        
              if(rdma_get_cm_event(this->_ec, &event)) {
                printf("Event poll unsuccesful, reason %d %s\n", errno, strerror(errno));
                exit(1);
              }
 
              switch (event->event) {

                case RDMA_CM_EVENT_CONNECT_REQUEST:
                  has_pending=1;
     
                  id = event->id;  

                  if(event->param.conn.private_data_len){
                    printf("connect request had data with it: %u bytes\n",event->param.conn.private_data_len);
                    connect_buffer = malloc(event->param.conn.private_data_len);
                    memcpy(connect_buffer,event->param.conn.private_data,event->param.conn.private_data_len);
                  }
 
                  break;
                case RDMA_CM_EVENT_ESTABLISHED: {
                  printf("connection is esteblished for id %p\n",event->id);

   
                    }
                  break;
                case RDMA_CM_EVENT_DISCONNECTED:
                  printf("connection is disconnected for id %p\n",event->id);
                  break;
                case RDMA_CM_EVENT_ADDR_ERROR:
                case RDMA_CM_EVENT_ROUTE_ERROR:
                case RDMA_CM_EVENT_CONNECT_ERROR:
                case RDMA_CM_EVENT_UNREACHABLE:
                case RDMA_CM_EVENT_REJECTED:
                case RDMA_CM_EVENT_ADDR_RESOLVED:
                case RDMA_CM_EVENT_ROUTE_RESOLVED:
                  printf("[RDMAPassive]: Unexpected to receive;\n");
                  break;


                case RDMA_CM_EVENT_DEVICE_REMOVAL:
                  printf("[TODO]:  need to disconnect everything;\n");

                  break;
                default:
                  break;
            }
            rdma_ack_cm_event(event);
        }
        return std::make_pair(id, connect_buffer);
    }
};