#pragma once
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <set>


struct fs_reply{
  uint32_t base;
  uint32_t page_id;
  uint32_t rkey;
  uint32_t user_data;
};

class VerbsEP{
public:
  struct rdma_cm_id * const id;

  struct ibv_qp * const qp;
  struct ibv_pd * const pd;
  const uint32_t max_inline_data;
  const uint32_t max_send_size;
  const uint32_t max_recv_size;


  uint32_t delayed_recv = 0;
  std::set<uint32_t> used_pages;

  VerbsEP(struct rdma_cm_id *id, uint32_t max_inline_data, uint32_t max_send_size, uint32_t max_recv_size): 
          id(id), qp(id->qp), pd(qp->pd), max_inline_data(0), max_send_size(max_send_size), max_recv_size(max_recv_size)
  {
      // empty
  }

  ~VerbsEP(){
    // empty
  }

  

  int akk_page(struct fs_reply* rep){
      used_pages.insert(rep->page_id);
      struct ibv_send_wr wr, *bad;
      struct ibv_sge sge = { (uint64_t)(void*)rep, sizeof(struct fs_reply), 0};
      wr.wr_id = 0;
      wr.next = NULL;
      wr.sg_list = &sge;
      wr.num_sge = 1;
      wr.opcode = IBV_WR_SEND;
      wr.send_flags = IBV_SEND_INLINE | IBV_SEND_SIGNALED;    // todo optimize. Signal sometimes.
      return ibv_post_send(this->qp, &wr, &bad);  
  }

  
  void evict_page(uint32_t page_id){
     used_pages.erase(page_id);
  }


  int delayed_post_recv(struct ibv_recv_wr* wrs, uint32_t len, struct ibv_recv_wr** bad_wr){
    delayed_recv++;
    if(len == delayed_recv){
      delayed_recv=0;
      return ibv_post_recv(this->qp,wrs,bad_wr);
    }
    return 0;
  }   
 
 
  enum rdma_cm_event_type get_event(){
      int ret;
      struct rdma_cm_event *event;
      
      ret = rdma_get_cm_event(id->channel, &event);
      if (ret) {
          perror("rdma_get_cm_event");
          exit(ret);
      }
      enum rdma_cm_event_type out = event->event;
     /* switch (event->event){
          case RDMA_CM_EVENT_ADDR_ERROR:
          case RDMA_CM_EVENT_ROUTE_ERROR:
          case RDMA_CM_EVENT_CONNECT_ERROR:
          case RDMA_CM_EVENT_UNREACHABLE:
          case RDMA_CM_EVENT_REJECTED:
   
               text(log_fp,"[rdma_get_cm_event] Error %u \n",event->event);
              break;

          case RDMA_CM_EVENT_DISCONNECTED:
              text(log_fp,"[rdma_get_cm_event] Disconnect %u \n",event->event);
              break;

          case RDMA_CM_EVENT_DEVICE_REMOVAL:
              text(log_fp,"[rdma_get_cm_event] Removal %u \n",event->event);
              break;
          default:
              text(log_fp,"[rdma_get_cm_event] %u \n",event->event);

      }*/
      rdma_ack_cm_event(event);
      return out;
  }    

  inline int write(struct ibv_sge* sges, uint32_t sgelen, uint32_t rkey, uint64_t remote_addr, uint64_t wr_id){        
      struct ibv_send_wr wr, *bad;

      wr.wr_id = wr_id;
      wr.next = NULL;
      wr.sg_list = sges;
      wr.num_sge = sgelen;
      wr.opcode = IBV_WR_RDMA_WRITE;

      wr.send_flags = IBV_SEND_SIGNALED;   
 
      wr.wr.rdma.remote_addr = remote_addr;
      wr.wr.rdma.rkey        = rkey;

      return ibv_post_send(this->qp, &wr, &bad);  
  }

  inline int read(struct ibv_sge* sges, uint32_t sgelen, uint32_t rkey, uint64_t remote_addr, uint64_t wr_id){        

 
      struct ibv_send_wr wr, *bad;

      wr.wr_id = wr_id;
      wr.next = NULL;
      wr.sg_list = sges;
      wr.num_sge = sgelen;
      wr.opcode = IBV_WR_RDMA_READ;

      wr.send_flags = IBV_SEND_SIGNALED;   
 
      wr.wr.rdma.remote_addr = remote_addr;
      wr.wr.rdma.rkey        = rkey;

      return ibv_post_send(this->qp, &wr, &bad);  
  }

};
