#include <iostream>       // std::cout
#include <infiniband/verbs.h>
#include <chrono>
#include "cxxopts.hpp"
#include  "ServerRDMA.hpp"
#include  "VerbsEP.hpp"
#include  <unordered_map>


#define MIN(a,b) (((a)<(b))?(a):(b))
#define CHECK_BIT(var,pos) ((var) & (1<<(pos)))
const uint32_t UD_HEADER = 40;
const uint32_t MEM_ALIGNMENT = 16; // in bits
const uint64_t ONE_GB = 1ULL << 30;
const uint32_t PAGES_IN_ONE_GB = (uint32_t)(ONE_GB >> 12);


struct context_t{
  
  struct ibv_pd   *pd;  
  std::vector<VerbsEP*> qps;

  struct ibv_cq *maincq; 

  // for finitswap we ned UD connection
  struct ibv_qp   *udqp; 
  std::unordered_map<uint32_t, uint32_t> ud_to_idrc;
  std::unordered_map<uint32_t, uint32_t> rc_to_idrc;


  // w ecan use SRQ
  struct ibv_srq *srq = NULL;



  // memory 
  char* buf;
  struct ibv_mr * mr;
};

// now we can create the connection.

struct context_t main_ctx;


int work_finiteswap(uint32_t pages, uint32_t max_srq){
    std::chrono::seconds sec(5);
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

    struct ibv_mr* null_mr = ibv_alloc_null_mr(main_ctx.pd); // for trimming useless part.

    if(null_mr == NULL){
      printf("null mr is not supported.  \n");
      exit(1);
    }

    struct ibv_recv_wr pagewr = {};
    struct ibv_recv_wr *bad_recv_wr;

    struct ibv_sge sges[2];
    sges[0] = {(uint64_t)null_mr->addr,UD_HEADER,null_mr->lkey}; // NULL wr is used to trim UD header
    // post all pages

    uint32_t can_post = MIN(8192,pages);
    std::vector<uint32_t> unused_pages;

    for(uint32_t i=0; i < can_post; i++){
      sges[1].addr = (uint64_t)main_ctx.buf+i*4096;
      sges[1].length = 4096;
      sges[1].lkey = main_ctx.mr->lkey;
      pagewr.wr_id=(uint64_t)main_ctx.buf+i*4096;
      pagewr.next=NULL;
      pagewr.sg_list=&sges[0];
      pagewr.num_sge = 2;
      int ret = ibv_post_recv(main_ctx.udqp, &pagewr, &bad_recv_wr);
      if (ret) {
        fprintf(stderr, "Couldn't post receive.\n");
        return 1;
      }
    }

    for(uint32_t i=can_post; i < pages; i++){
      unused_pages.push_back(i);
    }

    can_post = 0;


    struct ibv_recv_wr * empty_recvs = (struct ibv_recv_wr *)malloc(16*sizeof(struct ibv_recv_wr));
    for(uint32_t i =0; i<16; i++){
      empty_recvs[i].wr_id      = 0;
      empty_recvs[i].sg_list    = NULL; 
      empty_recvs[i].num_sge    = 0; 
      empty_recvs[i].next       = &empty_recvs[i+1]; // chain to the next
    }
    empty_recvs[16-1].next  = NULL; // the last must be null

    if(!main_ctx.srq){
      // for qp in qps. post_recv
      for(VerbsEP *ep : main_ctx.qps){
        // todo more than 16.
        for(uint32_t i=0; i < ep->max_recv_size/16; i++)
        
          if(ibv_post_recv(ep->qp,empty_recvs,&bad_recv_wr)){
            printf("Faield to post recives on RC\n");
          }
      }
    }
    
    uint32_t srq_num = max_srq;

    struct ibv_wc wc[32];
    while(true){ 

        while(srq_num >= 16){
          srq_num-=16;
          if(ibv_post_srq_recv(main_ctx.srq,empty_recvs,&bad_recv_wr)){
            fprintf(stderr, "Couldn't post shared receive.\n");
            return 1;
          }
        }  
        
        // todo make poll blocking
        int ne = ibv_poll_cq(main_ctx.maincq, 16, wc); // 32 is the length of the array wc. it means that we get at most 32 events. // why 32?
        if(ne<0){
          fprintf(stderr, "Error on ibv_poll_cq\n");
          return 1;
        }
   
        for(int i = 0; i < ne; i++){ // process received events
          uint64_t wr_id = wc[i].wr_id;
  //        printf("got an event %d\n",wc[i].opcode);

          if(!(wc[i].opcode & IBV_WC_RECV)){ // it is write
            continue;
          }

          if(wc[i].qp_num == main_ctx.udqp->qp_num){ // event on UDcq
            
            uint32_t rem_udp_qpn = wc[i].src_qp;
            uint32_t imm_data = wc[i].imm_data;
    //        printf("received UD request with imm_data %x\n",imm_data);
            if(wc[i].status == IBV_WC_SUCCESS){
              // succesfull swap out
              uint32_t page_id = (uint32_t)((wr_id - (uint64_t)main_ctx.buf)/4096);
              struct fs_reply rep = {(uint32_t)((uint64_t)main_ctx.buf>>MEM_ALIGNMENT), page_id , main_ctx.mr->rkey,imm_data} ;
              main_ctx.qps[main_ctx.ud_to_idrc[rem_udp_qpn]]->akk_page(&rep);
   //           printf("send  imm_data back %x\n",rep.user_data);
            } else {
              printf("[TODO] failed receive\n");
            }

            can_post++;
          } else {
            // received from client over reliable coonection;

      //      printf("Get evicted page\n");
            uint32_t local_qpn = wc[i].qp_num;
            uint32_t page_id = wc[i].imm_data;
            //evict requests
            
            VerbsEP *ep = main_ctx.qps[main_ctx.rc_to_idrc[local_qpn]];
            ep->evict_page(page_id);
            
            unused_pages.push_back(page_id);


            if(main_ctx.srq){
              srq_num++;
            }else{

              if (ep->delayed_post_recv(empty_recvs,16,&bad_recv_wr)) {
                fprintf(stderr, "Couldn't post receive to RCQP.\n");
                return 1;
              }
            }

          }
        }

        if(can_post && !unused_pages.empty()){
            can_post--;

            uint64_t page = (uint64_t)main_ctx.buf+((uint64_t)unused_pages.back())*4096;
            sges[1].addr = page;
            sges[1].length = 4096;
            sges[1].lkey = main_ctx.mr->lkey;
            pagewr.wr_id=sges[1].addr;
     
            int ret = ibv_post_recv(main_ctx.udqp, &pagewr, &bad_recv_wr);
            if (ret) {
              fprintf(stderr, "Couldn't post receive to UDQP.\n");
              return 1;
            }
            unused_pages.pop_back();
        }

        auto end = std::chrono::steady_clock::now();
        if(begin+sec < end){
          // -1 below to account for the last empty recv.
          uint64_t ttime = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
         // printf("Received %u shots in %lu us. ",shots, ttime);
          //printf("Throughput(Kreq/sec):%.2f ",(shots*1000.0)/ttime );
          //printf("BW(Gbit/sec):%.2f \n",(shots*1000.0)/ttime * shot_size/1024.0*1000.0/1024.0*8/1024.0 );
          begin = end;
        }
    } 
}



int work_infiniteswap(uint32_t pages, uint32_t max_srq){
    std::chrono::seconds sec(5);
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

    std::vector<uint64_t> free_regions;
    for(uint64_t i=0; i < pages/PAGES_IN_ONE_GB; i++){
      uint64_t region = (uint64_t)main_ctx.buf+i*ONE_GB;
      free_regions.push_back(region);
    }
    
    struct ibv_recv_wr *bad_recv_wr;  

    struct ibv_recv_wr * empty_recvs = (struct ibv_recv_wr *)malloc(16*sizeof(struct ibv_recv_wr));
    for(uint32_t i =0; i<16; i++){
      empty_recvs[i].wr_id      = 0;
      empty_recvs[i].sg_list    = NULL; 
      empty_recvs[i].num_sge    = 0; 
      empty_recvs[i].next       = &empty_recvs[i+1]; // chain to the next
    }
    empty_recvs[16-1].next  = NULL; // the last must be null

    if(!main_ctx.srq){
      // for qp in qps. post_recv
      for(VerbsEP *ep : main_ctx.qps){
        // todo more than 16.
        for(uint32_t i=0; i < ep->max_recv_size/16; i++)
          ibv_post_recv(ep->qp,empty_recvs,&bad_recv_wr);
      }
    } 

    uint32_t srq_num = max_srq;

    struct ibv_wc wc[32];
    while(true){ 

        while(srq_num >= 16){
          srq_num-=16;
          if(ibv_post_srq_recv(main_ctx.srq,empty_recvs,&bad_recv_wr)){
            fprintf(stderr, "Couldn't post shared receive.\n");
            return 1;
          }
        }  
        
        // todo make poll blocking
        int ne = ibv_poll_cq(main_ctx.maincq, 16, wc); // 32 is the length of the array wc. it means that we get at most 32 events. // why 32?
        if(ne<0){
          fprintf(stderr, "Error on ibv_poll_cq\n");
          return 1;
        }
   
        for(int i = 0; i < ne; i++){ // process received events
          //uint64_t wr_id = wc[i].wr_id;
      //    printf("got an event %d\n",wc[i].opcode);

          if(!(wc[i].opcode & IBV_WC_RECV)){ // it is write
            continue;
          }

          uint32_t local_qpn = wc[i].qp_num;
          uint32_t imm_data = wc[i].imm_data;
          VerbsEP *ep = main_ctx.qps[main_ctx.rc_to_idrc[local_qpn]];

          bool is_alloc = CHECK_BIT(imm_data,31);

          if(is_alloc){
            uint64_t region = free_regions.back();
            free_regions.pop_back();
            uint32_t region_id = (uint32_t)((region - (uint64_t)main_ctx.buf)/ONE_GB);
            struct fs_reply rep = {(uint32_t)((uint64_t)main_ctx.buf>>MEM_ALIGNMENT), region_id , main_ctx.mr->rkey, imm_data} ;
            ep->akk_page(&rep);
          }else{
            // user returns 1 GB
            //evict requests
            uint32_t region_id = imm_data;
            ep->evict_page(region_id);
            uint64_t region = (uint64_t)main_ctx.buf+((uint64_t)region_id)*ONE_GB;
            free_regions.push_back(region);
          }


          if(main_ctx.srq){
            srq_num++;
          }else{
            int ret = ep->delayed_post_recv(empty_recvs,16,&bad_recv_wr);
            if (ret) {
              fprintf(stderr, "Couldn't post receive to RCQP.\n");
              return 1;
            }
          } 
        }

        auto end = std::chrono::steady_clock::now();
        if(begin+sec < end){
          // -1 below to account for the last empty recv.
          uint64_t ttime = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
         // printf("Received %u shots in %lu us. ",shots, ttime);
          //printf("Throughput(Kreq/sec):%.2f ",(shots*1000.0)/ttime );
          //printf("BW(Gbit/sec):%.2f \n",(shots*1000.0)/ttime * shot_size/1024.0*1000.0/1024.0*8/1024.0 );
          begin = end;
        }
    } 


 
}


int connect_clients(ServerRDMA * server, uint32_t max_recv_size, uint32_t pages, uint32_t max_srq, uint32_t total_clients, bool infinite){
    main_ctx.srq = NULL;
    main_ctx.buf = NULL;    

    main_ctx.pd = server->getPD();

    if(max_srq){
      printf("create SRQ");
      struct ibv_srq_init_attr srq_init_attr = {};
      srq_init_attr.attr.max_wr = max_srq;
      srq_init_attr.attr.max_sge = 0; // only empty messages;

      main_ctx.srq = ibv_create_srq(main_ctx.pd,&srq_init_attr);
      if(!main_ctx.srq){
        printf("Failed to create SRQ.\n");
        return 1;
      }  
    } else{
      printf("NO shared receive\n");
    }
    
 
    // todo make blocking
    uint32_t cq_size =  max_srq + 128 + 8192;
    printf("Create main_cq with sixe %u \n",cq_size);

    main_ctx.maincq = ibv_create_cq(main_ctx.pd->context, cq_size, NULL, NULL, 0); 
    if (!main_ctx.maincq) {
      fprintf(stderr, "Couldn't create CQ\n");
      return 1;
    }


    if(!infinite){  // create udqp
 
      struct ibv_qp_init_attr init_attr;
      memset(&init_attr,0,sizeof init_attr);
      init_attr.send_cq = main_ctx.maincq;  // use a single CQ for send and recv
      init_attr.recv_cq = main_ctx.maincq;  // see above 
      init_attr.cap.max_send_wr  = 0; 
      init_attr.cap.max_recv_wr  = 8192;  
      init_attr.cap.max_send_sge = 0;  
      init_attr.cap.max_recv_sge = 2;  
      init_attr.qp_type = IBV_QPT_UD;
      
      main_ctx.udqp=server->create_ud_qp(&init_attr);
      printf("UDQP can receive. Info:\nQPN: %u\n",main_ctx.udqp->qp_num);
    }


    server->query_all_gids();
    
    main_ctx.buf = (char*)aligned_alloc((1<<MEM_ALIGNMENT),pages*4096);  // it is allocated aligned )
    
    printf("Allocated addres %p, base:%lx \n", main_ctx.buf, ((uint64_t)main_ctx.buf) >> MEM_ALIGNMENT);
    // i am trying to fit memory address in few bits. 
    if(((uint64_t)main_ctx.buf) >> MEM_ALIGNMENT != (uint32_t)(((uint64_t)main_ctx.buf) >> MEM_ALIGNMENT) || (((uint64_t)main_ctx.buf) & 0xffff)  != 0 ) {
      // buf should be at most 48 bit address. and it shpuld be MEM_ALIGNMENT bit alligned. SO we can use 32 bits as imm_data later
      printf("wrong allignment\n");
      return 1;
    }

    main_ctx.mr = ibv_reg_mr(main_ctx.pd, main_ctx.buf, pages*4096,  IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ); // IBV_ACCESS_LOCAL_WRITE means for sending and receiving. 
    if (!main_ctx.mr) {
      fprintf(stderr, "Couldn't register MR\n");
      return 1;
    }

    printf("Wait for clients of my memory %p %u\n",main_ctx.buf,main_ctx.mr->rkey);
    for(uint32_t i=0; i<total_clients; i++){
        struct rdma_cm_id* id;
        void* buf = NULL;

        std::tie(id,buf) = server->get_connect_request();
        if(buf == NULL && !infinite) {
          printf("Error received null memory on connect\n");
          exit(1);
        }
        
        struct ibv_qp_init_attr attr;
        memset(&attr, 0, sizeof(attr));
        attr.send_cq = main_ctx.maincq;
        attr.recv_cq = main_ctx.maincq;
        attr.cap.max_send_wr = 128; // todo tune
        attr.cap.max_recv_wr = main_ctx.srq ? 0 : max_recv_size; 
        attr.cap.max_send_sge = 1;  
        attr.cap.max_recv_sge = main_ctx.srq ? 0 : 1;
        attr.cap.max_inline_data = 16;
        attr.srq = main_ctx.srq;
        attr.qp_type = IBV_QPT_RC;
        attr.sq_sig_all = 0;

        if (rdma_create_qp(id, main_ctx.pd, &attr)) {
            perror("rdma_create_qp");
            exit(1);
        }
        
        
        uint32_t qpid = main_ctx.qps.size();
        main_ctx.rc_to_idrc.insert({id->qp->qp_num,qpid});

        if(!infinite){
          uint32_t rem_ud_qpn = *(uint32_t *)buf;
          printf("Accept a server with remote udqpn %u\n",rem_ud_qpn);
          free(buf);
          main_ctx.ud_to_idrc.insert({rem_ud_qpn,qpid});
        }


        main_ctx.qps.push_back(new VerbsEP(id, attr.cap.max_inline_data, attr.cap.max_send_wr, attr.cap.max_recv_wr )); 

        struct rdma_conn_param conn_param;
        memset(&conn_param, 0 , sizeof(conn_param));
        conn_param.responder_resources = 16; // up to 16 reads
        conn_param.initiator_depth = 0;
        conn_param.retry_count = 3;
        conn_param.rnr_retry_count = 3; 

        if(rdma_accept(id, &conn_param)){
          printf(" failed to accept\n"); 
          exit(1);
        }
        printf("Accepted one\n");
        if(!infinite){
          server->send_gid_info(id);
        }
    }
    printf("all clients connected\n");
    return 0;
}



// ParseResult class: used for the parsing of line arguments
cxxopts::ParseResult
parse(int argc, char* argv[])
{
    cxxopts::Options options(argv[0], "Sender of UD test");
    options
      .positional_help("[optional args]")
      .show_positional_help();

  try
  {
 
    options.add_options()
      ("a,address", "IPADDR", cxxopts::value<std::string>(), "IP")
      ("recv-size", "The IB maximum receive size in pages", cxxopts::value<uint32_t>()->default_value(std::to_string(64)), "N") // this is the size of the queue request on the device
      ("pages", "How many pages to use", cxxopts::value<uint32_t>()->default_value(std::to_string(256)), "N") // this is the size of the queue request on the device
      ("srq-size", "the size of srq for all rc connections. 0 means no srq", cxxopts::value<uint32_t>()->default_value(std::to_string(2048)), "N") // this is the size of the queue request on the device
      ("clients", "clients to expect", cxxopts::value<uint32_t>()->default_value(std::to_string(1)), "N") // this is the size of the queue request on the device
      ("i,infiniswap", "use infiniswap algorithm") // this is the size of the queue request on the device
      ("help", "Print help")
     ;
 
    auto result = options.parse(argc, argv);
    if (result.count("address") == 0)
    {
      std::cout <<"No address is provided" << options.help({""}) << std::endl;
      exit(0);
    }
    if (result.count("help"))
    {
      std::cout << options.help({""}) << std::endl;
      exit(0);
    }
 
    return result;

  }
  catch (const cxxopts::OptionException& e)
  {
    std::cout << "error parsing options: " << e.what() << std::endl;
    std::cout << options.help({""}) << std::endl;
    exit(1);
  }
}

int main(int argc, char* argv[]){
// parse the arguments and creates a dictionary which maps arguments to values
    auto allparams = parse(argc,argv);

    

    std::string ip = allparams["address"].as<std::string>(); // "192.168.1.20"; .c_str()
    int port = 9999;
 
    ServerRDMA * server = new ServerRDMA(const_cast<char*>(ip.c_str()),port);
    

    uint32_t max_recv_size = allparams["recv-size"].as<uint32_t>();  

    bool infinite = allparams.count("infiniswap");
    uint32_t srq_size = allparams["srq-size"].as<uint32_t>();

    uint32_t pages =  allparams["pages"].as<uint32_t>();  ;
    pages = ((pages - 1)/PAGES_IN_ONE_GB + 1) * PAGES_IN_ONE_GB;
    printf("We use %u pages, that are %u GB\n",pages, pages/PAGES_IN_ONE_GB);

    if(infinite) printf("Use infini\n"); else printf("use finitswap\n");


    connect_clients(server,max_recv_size, pages, srq_size, allparams["clients"].as<uint32_t>(), infinite);

    printf("start loop\n");

    if(infinite){
      work_infiniteswap(pages,srq_size);
    }  else {
      work_finiteswap(pages,srq_size);
    }
    

  
}
 
