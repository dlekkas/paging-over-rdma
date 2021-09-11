#pragma once

#include <deque>


const uint64_t ONE_GB = 1ULL << 30;
const uint32_t PAGES_IN_ONE_GB = (uint32_t)(ONE_GB >> 12);

struct rem_t{
	uint32_t page_id;
	uint8_t  num_pages;
	uint8_t  segment_id;
	uint16_t res2;
};

struct req_t{
	uint8_t sent_pages;
	uint8_t issued_sents;
	uint8_t completions;
	uint8_t num_pages;
	uint64_t local_addr;

	std::vector<rem_t> remote_addrs; // 
};

class Swap {
public:
 
    virtual ~Swap() = default;

    virtual void connect(std::string ip, uint16_t port) = 0;

    virtual void swapin(req_t* request) = 0;
    virtual void swapout(req_t* request) = 0;


    virtual std::pair<uint32_t,uint32_t> meminfo() = 0;;
    virtual int evict(uint32_t numpages) = 0;

    // pushed completion in dequeue
    virtual int poll(std::deque<req_t*> &completed) = 0;

    virtual void print_stats() = 0;
};



class FiniteSwap: public Swap {
	uint32_t total_pages = 0;


	uint64_t base_addr = 0;
	uint32_t remote_key = 0;
	uint32_t remote_qpn;
	std::vector<uint32_t> free_pages;


	std::vector<uint16_t> slots;

	std::vector<req_t*> pending;	

	// now we can create the connection.
	struct ibv_qp   *udqp = NULL;
	VerbsEP* _ep = NULL;
	struct ibv_cq *maincq = NULL;
	
	struct ibv_ah *ah = NULL;
	

	char* buf = NULL;
	struct ibv_mr *mr = NULL;




	struct ibv_send_wr wrs[32];
	struct ibv_sge sges[32];
public:
 	struct ibv_pd *pd = NULL;
 	FiniteSwap(){
 		
 		pending.resize(32);
 		for(uint16_t i=0; i<32;i++) slots.push_back(i);
 	};

 	void install_memory(uint32_t lkey){
 		for(uint32_t i=0; i < 32; i++){
 			sges[i].lkey = lkey;
 			sges[i].length = 4096;
 			wrs[i].sg_list    = &sges[i];
 			wrs[i].next = &wrs[i+1];
 			wrs[i].send_flags = IBV_SEND_SIGNALED;
 		}

 		uint32_t req_size = sizeof(struct fs_reply);
 		uint32_t num = 4096/req_size;

 		for(uint32_t i=0; i < num; i++){
 			struct ibv_recv_wr wr = {};
		    struct ibv_recv_wr *bad_wr;
		    struct ibv_sge sge = {};

		    sge.addr = (uint64_t)buf + i*req_size;
		    sge.length = req_size;
		    sge.lkey = mr->lkey;
		    wr.wr_id=i;
		    wr.next=NULL;
		    wr.sg_list=&sge;
		    wr.num_sge = 1;
 			int ret = ibv_post_recv(_ep->qp,&wr,&bad_wr);
 			if(ret) printf("faield to post recveives");
 		}
 	};

    void connect(std::string ip, uint16_t port) override{
	    struct rdma_cm_id * id = ClientRDMA::sendConnectRequest((char*)ip.c_str(),port);
	    pd = id->pd ? id->pd : ibv_alloc_pd(id->verbs);

		maincq = ibv_create_cq(pd->context, 256 , NULL, NULL, 0); 
	    if (!maincq) {
	      fprintf(stderr, "Couldn't create CQ\n");
	      return;
	    }

	    struct ibv_qp_init_attr init_attr = {};
	    init_attr.send_cq = maincq;  // use a single CQ for send and recv
	    init_attr.recv_cq = maincq;  // see above 
	    init_attr.cap.max_send_wr  = 128; 
	    init_attr.cap.max_recv_wr  = 0;  
	    init_attr.cap.max_send_sge = 1;  
	    init_attr.cap.max_recv_sge = 0;  
	    init_attr.qp_type = IBV_QPT_UD;
	    udqp = ClientRDMA::create_ud(id,pd,&init_attr);
	    printf("Created ud with %u\n",udqp->qp_num);

		 
	    memset(&init_attr, 0, sizeof(init_attr));
	    init_attr.send_cq = maincq;  // use a single CQ for send and recv
	    init_attr.recv_cq = maincq;  // see above 
	    init_attr.cap.max_send_wr = 16;
	    init_attr.cap.max_recv_wr = 256;
	    init_attr.cap.max_send_sge = 1;
	    init_attr.cap.max_recv_sge = 1;
	    init_attr.cap.max_inline_data = 16;
	    init_attr.qp_type = IBV_QPT_RC;

	    if (rdma_create_qp(id, pd, &init_attr)) {
	        perror("rdma_create_qp");
	        exit(1);
	    }
		      

	    _ep = new VerbsEP(id, init_attr.cap.max_inline_data, init_attr.cap.max_send_wr, init_attr.cap.max_recv_wr ); 
	    
	 
	    struct rdma_conn_param conn_param;
	    memset(&conn_param, 0 , sizeof(conn_param));
	    conn_param.responder_resources = 0;
	    conn_param.initiator_depth = 16;
	    conn_param.retry_count = 3;
	    conn_param.rnr_retry_count = 3; 
	    conn_param.private_data = (void*)&(udqp->qp_num);
	    conn_param.private_data_len = sizeof(uint32_t);
	   
	    buf = (char*)aligned_alloc(4096,4096);  
	    mr = ibv_reg_mr(pd, buf, 4096,  IBV_ACCESS_LOCAL_WRITE);
	    if (!mr) {
	      fprintf(stderr, "Couldn't register MR\n");
	      return;
	    }


	    struct ibv_recv_wr wr = {};
	    struct ibv_recv_wr *bad_wr;
	    struct ibv_sge sge = {};

	    sge.addr = (uint64_t)buf;
	    sge.length = 4096;
	    sge.lkey = mr->lkey;
	    wr.wr_id=0;
	    wr.next=NULL;
	    wr.sg_list=&sge;
	    wr.num_sge = 1;

	    if (ibv_post_recv(_ep->qp, &wr, &bad_wr)) {
	      fprintf(stderr, "Couldn't post receive.\n");
	      return;
	    }

	    if(rdma_connect(_ep->id, &conn_param)){
	      printf("Failed to connect\n");
	      exit(1);
	    }

	    struct ibv_wc wc;

	    while(ibv_poll_cq(maincq, 1, &wc) == 0){

	    }

	    if(!(wc.opcode & IBV_WC_RECV)){ // it is write
	      printf("Error in ops\n"); // todo explain better
	      exit(1);
	    }

	      
	    remote_qpn =  *(uint32_t *)buf;
	    uint32_t rgids_num =  *(((uint32_t *)buf)+1);
	    printf("Received %u gids\n",rgids_num);

	    union ibv_gid   *dgids = (union ibv_gid*)(void*)(((uint32_t *)buf)+2); // address of the destination.
	    uint32_t gid_id = 3;
	 
	    struct ibv_ah_attr ah_attr;
	    memset(&ah_attr,0,sizeof ah_attr);

	    printf("We pick gid_id %u slid %u \n", gid_id, wc.slid);
	   
	    ah_attr.is_global = wc.slid ? 0 : 1; // must be 1 for roce
	    ah_attr.dlid = wc.slid;      // must be 0 for roce
	    ah_attr.sl  = 0; // it is a service level. I am not sure what it gives for us, but can be 0.
	    ah_attr.src_path_bits = 0; // I do not know. what is it. must be zero
	    ah_attr.port_num      = id->port_num; 
	    ah_attr.grh.hop_limit = 1; 

	// address of the remote side
	    ah_attr.grh.dgid = dgids[gid_id];
	    printf("GID[%d].  subnet %llu\n\t inteface %llu\n",gid_id,dgids[gid_id].global.subnet_prefix, dgids[gid_id].global.interface_id);
	    ah_attr.grh.sgid_index = gid_id; //  it sets ip and roce versions
	 
	    ah = ibv_create_ah(pd, &ah_attr);
	    if(ah==NULL){
	      printf("failed to create ah\n");
	      exit(1);
	    }
	    printf("Connected finit swap. \n");
    }



    inline void send_batch_rc(struct req_t *request){
    	
    	uint8_t messages = 0;
    	for( request->sent_pages=0 ; request->sent_pages< request->num_pages; request->sent_pages++ ){
    		if(free_pages.empty()) break;
    		uint32_t page_id = free_pages.back();
    		sges[messages].addr = request->local_addr + 4096*(uint32_t)request->sent_pages;
    		sges[messages].length = 4096;
    		free_pages.pop_back();
    		request->remote_addrs.push_back({page_id,1,0,0});
    		wrs[messages].num_sge=1;
    		wrs[messages].wr_id      = (uint64_t)(void*)request;
    		wrs[messages].opcode     = IBV_WR_RDMA_WRITE;
			wrs[messages].wr.rdma.remote_addr = base_addr + page_id*4096;
			wrs[messages].wr.rdma.rkey        = remote_key;
    		messages++;
    	}

    	assert(messages <= 16 && messages!=0);
    	wrs[messages-1].next = NULL;
    	struct ibv_send_wr *bad_send_wr;
    	int ret = ibv_post_send(_ep->qp, wrs, &bad_send_wr);
    	if(ret) printf("failes to send over RC\n");
    	
    	request->issued_sents = messages;
    	wrs[messages-1].next = &wrs[messages]; 
    };

    inline void send_batch_ud(struct req_t *request){
    	
    	uint8_t messages = 0;
		
		uint32_t slot = slots.back();
		slots.pop_back();
		pending[slot] = request;

    	for( ; request->sent_pages < request->num_pages; request->sent_pages++ ){

    		sges[messages].addr = request->local_addr + 4096*(uint32_t)request->sent_pages;

    		wrs[messages].wr_id      = (uint64_t)(void*)request;
    		wrs[messages].num_sge=1;
    		wrs[messages].imm_data   = (slot<<16) + request->sent_pages ; // user info
    		wrs[messages].opcode     = IBV_WR_SEND_WITH_IMM;
			//wrs[messages].wr.rdma.remote_addr = base_addr + page_id*4096;
			//wrs[messages].wr.rdma.rkey        = remote_key;
			wrs[messages].wr.ud.ah          = ah;
			wrs[messages].wr.ud.remote_qpn  = remote_qpn;
			wrs[messages].wr.ud.remote_qkey = 0x11111111;
    	//	printf("----------------send page[%u]. Imm data %x\n",request->sent_pages,wrs[messages].imm_data);
    		messages++;


    	}

    	assert(messages <= 16 && messages!=0);
    	wrs[messages-1].next = NULL;
		struct ibv_send_wr *bad_send_wr;
    	int ret = ibv_post_send(udqp, wrs, &bad_send_wr);
    	if(ret) printf("failes to send over RC\n");
    	
    	request->issued_sents = messages;
    	wrs[messages-1].next = &wrs[messages]; 
    };


    void swapin(struct req_t *request) override {
    	request->sent_pages = 0;
		request->issued_sents = 0;
		request->completions = 0;
    	
    	if(!free_pages.empty()){
  //  		printf("use rc for swap in\n");
    		send_batch_rc(request);
    	}

    	if(request->sent_pages!=request->num_pages){
  //  		printf("use ud for swap in\n");
    		send_batch_ud(request);
    	}
    };

    void swapout(struct req_t *request) override {
    	request->sent_pages = 0;
		request->issued_sents = 0;
		request->completions = 0;

    	uint8_t messages = 0;
    	
    	for( rem_t & rem: request->remote_addrs ){
    		sges[messages].addr = request->local_addr + 4096*(uint32_t)messages;
    		sges[messages].length = 4096;
  			wrs[messages].num_sge=1;
    		wrs[messages].wr_id      = (uint64_t)(void*)request;
    		wrs[messages].opcode     = IBV_WR_RDMA_READ;
			wrs[messages].wr.rdma.remote_addr = base_addr + ((uint32_t)rem.page_id)*4096;
			wrs[messages].wr.rdma.rkey        = remote_key;
	//		printf("Send read request to %lx %u\n",wrs[messages].wr.rdma.remote_addr, remote_key);
    		messages++;
    	}

    	assert(messages <= 16 && messages!=0);
    	wrs[messages-1].next = NULL;
    	struct ibv_send_wr *bad_send_wr;
    	int ret = ibv_post_send(_ep->qp, wrs, &bad_send_wr);
    	if(ret) printf("failes to send over RC\n");
    	
    	request->issued_sents = messages;
    	wrs[messages-1].next = &wrs[messages]; 
    };


    std::pair<uint32_t,uint32_t> meminfo() override{
    	return {total_pages,(uint32_t)free_pages.size()};
    };

    int evict(uint32_t numpages) override {
    	for( uint8_t i=0; i< numpages; i++){
    		uint32_t page_id = free_pages.back();
    		free_pages.pop_back();
    		wrs[i].num_sge    = 0;
    		wrs[i].wr_id      = 0;
    		wrs[i].opcode     = IBV_WR_RDMA_WRITE_WITH_IMM;
    		wrs[i].imm_data   = page_id;
    	}
   // 	printf("Evict %u \n",numpages);
    	assert(numpages <= 16 && numpages!=0);
    	wrs[numpages-1].next = NULL;
    	struct ibv_send_wr *bad_send_wr;
    	int ret = ibv_post_send(_ep->qp, wrs, &bad_send_wr);
    	if(ret) printf("failes to send over RC\n");

    	wrs[numpages-1].next = &wrs[numpages]; 

    	return 0;
    };

    // pushed completion in dequeue
    int poll(std::deque<req_t*> &completed) override {
    	struct ibv_wc wc[16];
    	int inserted = 0;

    	int n = ibv_poll_cq(maincq, 16, wc);

    	for(int i=0; i < n; i++){
    		req_t* request = NULL;
    		if(wc[i].status != IBV_WC_SUCCESS){
    			printf("wc Error %d\n",wc[i].status);
    		}
    		switch (wc[i].opcode){
    			case IBV_WC_SEND: {
    				// local pages was sent via UD for swapin.
    //				printf("sent is done via ud for %lx\n",wc[i].wr_id);

    				break;
    			};
				case IBV_WC_RECV: {
					// successfull swapin via UD.. We receive addr of the page. 
					
					
					struct fs_reply* rep = ((struct fs_reply*)buf)+wc[i].wr_id;
  //  				printf("get address %u %u %u %x\n",rep->base,rep->page_id,rep->rkey,rep->user_data);
    				remote_key = rep->rkey;
    				
    				uint32_t slot = (rep->user_data >> 16);
    				uint16_t local_page_id = (uint16_t)(rep->user_data & 0xffff);
    				base_addr = ((uint64_t)rep->base) << 16;


    				request = pending[slot];
	//				printf("Swapin was done for pageid[%u] of %p\n",local_page_id,request);
					request->completions++;
    				request->remote_addrs.push_back({rep->page_id,1,0,0});

    				total_pages++;

    				if(request->completions == request->issued_sents){
    					slots.push_back(slot);
    					pending[slot] = nullptr;
    				}

				 	struct ibv_recv_wr wr = {};
				    struct ibv_recv_wr *bad_wr;
				    struct ibv_sge sge = {};
				    uint32_t req_size = sizeof(struct fs_reply);
				    sge.addr = (uint64_t)buf + wc[i].wr_id*req_size;
				    sge.length = req_size;
				    sge.lkey = mr->lkey;
				    wr.wr_id=wc[i].wr_id;
				    wr.next=NULL;
				    wr.sg_list=&sge;
				    wr.num_sge = 1;
		 			int ret = ibv_post_recv(_ep->qp,&wr,&bad_wr);
		 			if(ret) printf("faield to post recv\n");

    				break;
    			};
    			case IBV_WC_RDMA_WRITE: {
    				// finished swapout or evict. evict has wrid = 0
    //				printf("-------sent is done via RC for %lx\n",wc[i].wr_id);
    				if(wc[i].wr_id != 0){
    					request = (req_t*)(void*)wc[i].wr_id;
    					request->completions++;
    				} else {
   // 					printf("-------Evict completion\n");
    				}
    				break;
    			};
    			case IBV_WC_RDMA_READ: {
    				// finished swapin
    //				printf("finished swap in\n");
    				request = (req_t*)(void*)wc[i].wr_id;
    				request->completions++;

    				if(request->completions == request->issued_sents){
    //					printf("All pages are swaped-in %p\n",request);
    					for(rem_t& rem: request->remote_addrs){
    						free_pages.push_back(rem.page_id);	
    					}	
    					request->remote_addrs.clear();
    				}

    				break;
    			};    			

    			default:
    				exit(1);
    		}

	    	if(request!=NULL && request->completions == request->issued_sents){
				completed.push_back(request);
				inserted++;
			}
    	}

    	return inserted;
    };

    void print_stats() override {

    }
};




	class InfiniSwap: public Swap {
 		// now we can create the connection.

		struct region_t
		{	
			region_t(uint32_t _pages):pages(_pages){
				
				memory.insert({0,_pages});
			}

			uint32_t pages;
			std::set<std::pair<uint32_t,uint32_t>> memory; // each elemt is start length

			std::pair<uint32_t,uint32_t> get_pages(uint32_t num){
				if(pages == 0) return {0,0}; 
				auto it = memory.begin();
				if(pages<num) { // we return only part
					uint32_t start = it->first;
					uint32_t len = it->second;
					memory.erase(it);
					pages-=len;
					return {start,len};
				}
				// we try to find contig
				for(; it != memory.end(); ++it) {
				    if(num <= it->second){
				    	uint32_t start = it->first;
				    	pages-=num;

				    	if(it->second!=num){
				    		memory.insert(it,{start+num,it->second - num});
				    	}
				    	
				    	memory.erase(it);
				    	return {start,num};
				    }
				}
				it = memory.begin();
				uint32_t start = it->first;
				uint32_t len = it->second;
				pages-=len;
				memory.erase(it);
				return {start,len};
			}


			void return_pages(uint32_t start, uint32_t len){
				pages+=len;
				if(memory.size() == 0){
					memory.insert({start,len});
					return;
				}
				auto nextit=memory.lower_bound ({start,len}); 
				if(nextit!=memory.begin()){
					auto previt=std::prev(nextit); 
					if(previt->first+previt->second == start){
						start=previt->first;
						len+=previt->second;
						memory.erase(previt);	
					}
				}
				if(start+len == nextit->first){
					len+=nextit->second;
					memory.erase(nextit);
				}

				memory.insert({start,len});
			}
		};

		
	uint64_t base_addr = 0;
	uint32_t remote_key = 0;

	bool has_pending_alloc = false;
	uint32_t total_regions = 0; 
	uint32_t free_pages = 0;


	std::vector<region_t*> all_regions;
	std::vector<req_t*> pending;


	VerbsEP* _ep = NULL;
	struct ibv_cq *maincq = NULL;
 
	struct ibv_send_wr wrs[32];
	struct ibv_sge sges[32];

	uint32_t pre_evicted = 0;

	char* buf = NULL;
	struct ibv_mr *mr = NULL;

	public:
	 	struct ibv_pd *pd = NULL;
	 	InfiniSwap(){

	 	};

	 	void install_memory(uint32_t lkey){
	 		for(uint32_t i=0; i < 32; i++){
	 			sges[i].lkey = lkey;
	 			sges[i].length = 4096;
	 			wrs[i].sg_list    = &sges[i];
	 			wrs[i].next = &wrs[i+1];
	 			wrs[i].send_flags = IBV_SEND_SIGNALED;
	 		}

	 		uint32_t req_size = sizeof(struct fs_reply);
	 		uint32_t num = 4096/req_size;

	 		for(uint32_t i=0; i < num; i++){
	 			struct ibv_recv_wr wr = {};
			    struct ibv_recv_wr *bad_wr;
			    struct ibv_sge sge = {};

			    sge.addr = (uint64_t)buf + i*req_size;
			    sge.length = req_size;
			    sge.lkey = mr->lkey;
			    wr.wr_id=i;
			    wr.next=NULL;
			    wr.sg_list=&sge;
			    wr.num_sge = 1;
	 			int ret = ibv_post_recv(_ep->qp,&wr,&bad_wr);
	 			if(ret) printf("faield to post recveives");
	 		}
	 	};

	    void connect(std::string ip, uint16_t port) override{
	    	printf("connect InfiniSwap\n");
		    struct rdma_cm_id * id = ClientRDMA::sendConnectRequest((char*)ip.c_str(),port);
		    pd = id->pd ? id->pd : ibv_alloc_pd(id->verbs);

			maincq = ibv_create_cq(pd->context, 256 , NULL, NULL, 0); 
		    if (!maincq) {
		      fprintf(stderr, "Couldn't create CQ\n");
		      return;
		    }

		    struct ibv_qp_init_attr init_attr = {};
			 
		    memset(&init_attr, 0, sizeof(init_attr));
		    init_attr.send_cq = maincq;  // use a single CQ for send and recv
		    init_attr.recv_cq = maincq;  // see above 
		    init_attr.cap.max_send_wr = 16;
		    init_attr.cap.max_recv_wr = 256;
		    init_attr.cap.max_send_sge = 1;
		    init_attr.cap.max_recv_sge = 1;
		    init_attr.cap.max_inline_data = 16;
		    init_attr.qp_type = IBV_QPT_RC;

		    if (rdma_create_qp(id, pd, &init_attr)) {
		        perror("rdma_create_qp");
		        exit(1);
		    }
			      

		    _ep = new VerbsEP(id, init_attr.cap.max_inline_data, init_attr.cap.max_send_wr, init_attr.cap.max_recv_wr ); 
		    
		 
		    struct rdma_conn_param conn_param;
		    memset(&conn_param, 0 , sizeof(conn_param));
		    conn_param.responder_resources = 0;
		    conn_param.initiator_depth = 16;
		    conn_param.retry_count = 3;
		    conn_param.rnr_retry_count = 3; 
		    conn_param.private_data = NULL;
		    conn_param.private_data_len = 0;
		   	
		   	buf = (char*)aligned_alloc(4096,4096);  
		    mr = ibv_reg_mr(pd, buf, 4096,  IBV_ACCESS_LOCAL_WRITE);
		    if (!mr) {
		      fprintf(stderr, "Couldn't register MR\n");
		      return;
		    }

		
		    if(rdma_connect(_ep->id, &conn_param)){
		      printf("Failed to connect\n");
		      exit(1);
		    }
	    }

	    inline int send_batch_rc(struct req_t *request){
	    	
	    	uint8_t messages = 0;
	    	uint8_t pages_to_send = request->num_pages - request->sent_pages;
	//    	printf("We try to send %u pages\n",pages_to_send);
	    	uint8_t try_id=0;
	    	while(pages_to_send){
	    		if(try_id == all_regions.size()){
	    			break;
	    		}
	    		if(all_regions[try_id]==nullptr ||  all_regions[try_id]->pages == 0){
	//    			printf("Segment[%u] has no memory\n",try_id);
	    			try_id++;
	    			continue;
	    		}

	    		auto range = all_regions[try_id]->get_pages(pages_to_send);
	  //  		printf("we got %u pages from the segment[%u].%u \n",range.second,try_id,range.first);


	    		sges[messages].addr = request->local_addr + 4096*(uint32_t)request->sent_pages;
	    		sges[messages].length = 4096*range.second;
	    		
	    		wrs[messages].num_sge=1;
	    		wrs[messages].wr_id      = (uint64_t)(void*)request;
	    		wrs[messages].opcode     = IBV_WR_RDMA_WRITE;
				wrs[messages].wr.rdma.remote_addr = base_addr + try_id*ONE_GB + range.first*4096;
				wrs[messages].wr.rdma.rkey        = remote_key;

	    		pages_to_send-=range.second;
	    		request->sent_pages+=range.second;
	    		request->remote_addrs.push_back({range.first,(uint8_t)range.second,try_id,0});

	    		messages++;
	    	}

	    	if(messages){
		    	assert(messages < 16);
		    	wrs[messages-1].next = NULL;
		    	struct ibv_send_wr *bad_send_wr;
		    	int ret = ibv_post_send(_ep->qp, wrs, &bad_send_wr);
		    	if(ret) printf("failes to send over RC\n");
		    	request->issued_sents = messages;
		    	wrs[messages-1].next = &wrs[messages]; 
	    	}

	    	if(pages_to_send){
	    		pending.push_back(request);
	    		return 1;
	    	} else {
	    		return 0;
	    	}
	    };

	    void request_mem(){
	//    	printf("need to request memory\n");
			has_pending_alloc = true;

    		wrs[0].num_sge    = 0;
    		wrs[0].wr_id      = 0;
    		wrs[0].opcode     = IBV_WR_RDMA_WRITE_WITH_IMM;
    		wrs[0].imm_data   = 1U<<31;
    		wrs[0].next = NULL;
    		struct ibv_send_wr *bad_send_wr;
    		int ret = ibv_post_send(_ep->qp, wrs, &bad_send_wr);
			if(ret) printf("failes to send over RC\n");
			wrs[0].next = &wrs[1];   
	    }

	    void swapin(struct req_t *request) override {
	    	request->sent_pages = 0;
			request->issued_sents = 0;
			request->completions = 0;
			
			if(free_pages == 0){
				
				if(!has_pending_alloc){
					request_mem();
	    		}
	    		pending.push_back(request);
	    		return;
	    	}

			if(send_batch_rc(request) && !has_pending_alloc){
				request_mem();
			}
	    };

	    void swapout(struct req_t *request) override {
	    	request->sent_pages = 0;
			request->issued_sents = 0;
			request->completions = 0;
			
			uint8_t messages = 0;
	    	uint32_t cur_page = 0;
	    	for( rem_t & rem: request->remote_addrs ){
	    		//rem.segment_id;
	    		sges[messages].addr = request->local_addr + 4096*cur_page;
	    		sges[messages].length = rem.num_pages*4096;
	  			wrs[messages].num_sge=1;
	    		wrs[messages].wr_id      = (uint64_t)(void*)request;
	    		wrs[messages].opcode     = IBV_WR_RDMA_READ;
				wrs[messages].wr.rdma.remote_addr = base_addr + (ONE_GB*rem.segment_id) + ((uint32_t)rem.page_id)*4096;
				wrs[messages].wr.rdma.rkey        = remote_key;
	    		messages++;
	    		cur_page+=rem.num_pages;
	    	}

	    	assert(messages < 16 && messages!=0);
	    	wrs[messages-1].next = NULL;
	    	struct ibv_send_wr *bad_send_wr;
	//    	printf("Send read request\n");

	    	int ret = ibv_post_send(_ep->qp, wrs, &bad_send_wr);
	    	if(ret) printf("failes to send over RC\n");
	    	
	    	request->issued_sents = messages;
	    	wrs[messages-1].next = &wrs[messages]; 
		};	  


		std::pair<uint32_t,uint32_t> meminfo() override{
	    	return {(uint32_t)total_regions*PAGES_IN_ONE_GB,  free_pages};
	    };

	    int evict(uint32_t numpages) override {
	    	pre_evicted+= numpages;

	    	if(pre_evicted>=PAGES_IN_ONE_GB){
	    		for(uint8_t region_id=0; region_id < all_regions.size(); region_id++){
	    			if(all_regions[region_id]!=nullptr && all_regions[region_id]->pages == PAGES_IN_ONE_GB ){
	    				wrs[0].num_sge=0;
			    		wrs[0].wr_id      = 0;
			    		wrs[0].opcode     = IBV_WR_RDMA_WRITE_WITH_IMM;
			    		wrs[0].imm_data   = region_id;	
			    		wrs[0].next = NULL;
				    	struct ibv_send_wr *bad_send_wr;
				    	int ret = ibv_post_send(_ep->qp, wrs, &bad_send_wr);
				    	if(ret) printf("failes to send over RC\n");
				    	wrs[0].next = &wrs[1];	

				    	delete all_regions[region_id];
				    	all_regions[region_id]=nullptr;
				    	free_pages-=PAGES_IN_ONE_GB;
				    	pre_evicted-=numpages;
						total_regions--;
						return 0;
	    			}
	    		}
	    		return 1;
	    	}
	    	return 0;

	    };

	    // pushed completion in dequeue
	    int poll(std::deque<req_t*> &completed) override {
	    	struct ibv_wc wc[16];
	    	int inserted = 0;

	    	int n = ibv_poll_cq(maincq, 16, wc);

	    	for(int i=0; i < n; i++){
	    		req_t* request = NULL;
	    		if(wc[i].status != IBV_WC_SUCCESS){
	    			printf("wc Error %d\n",wc[i].status);
	    		}
	    		switch (wc[i].opcode){
	    			case IBV_WC_SEND: {
	    				// local pages was sent via UD for swapin.
	   // 				printf("sent is done via RC for alloc/evict request\n");

	    				break;
	    			};
					case IBV_WC_RECV: {
						// successfull allocation. We receive addr of the page. 
						
			
						struct fs_reply* rep = ((struct fs_reply*)buf)+wc[i].wr_id;
	    //				printf("get address %u %u %u %u\n",rep->base,rep->page_id,rep->rkey,rep->user_data);
	    				uint32_t segment_id = rep->page_id;
	    				remote_key = rep->rkey;
	    				base_addr = ((uint64_t)rep->base) << 16;
	 
	    				has_pending_alloc = false;
	    				// try to allocate

	    				total_regions++;
	    				free_pages+=(PAGES_IN_ONE_GB) ;	

	    				if(all_regions.size() <= segment_id){
	    					all_regions.resize(segment_id+1);
	    				}

						all_regions[segment_id] = new region_t(PAGES_IN_ONE_GB);

		//				printf("We have %u pages\n", all_regions[segment_id]->pages);


						while(pending.size()){
		//					printf("push pending\n");
							req_t* request = pending.back();
							pending.pop_back();
							int ret = send_batch_rc(request); // it can still make a request pending
	    					if(ret){
	    //						printf("request memory again\n");
	    						request_mem();
	    						break;
	    					}
						}
	    				

					 	struct ibv_recv_wr wr = {};
					    struct ibv_recv_wr *bad_wr;
					    struct ibv_sge sge = {};
					    uint32_t req_size = sizeof(struct fs_reply);
					    sge.addr = (uint64_t)buf + wc[i].wr_id*req_size;
					    sge.length = req_size;
					    sge.lkey = mr->lkey;
					    wr.wr_id=wc[i].wr_id;
					    wr.next=NULL;
					    wr.sg_list=&sge;
					    wr.num_sge = 1;
			 			int ret = ibv_post_recv(_ep->qp,&wr,&bad_wr);
			 			if(ret) printf("faield to post recv\n");

	    				break;
	    			};
	    			case IBV_WC_RDMA_WRITE: {
	    				// finished swapout or evict. evict has wrid = 0
	   // 				printf("write is done for %lx\n",wc[i].wr_id);
	    				if(wc[i].wr_id != 0){
	    					request = (req_t*)(void*)wc[i].wr_id;
	    					request->completions++;
	    				} else {
	    //					printf("end alloc of evict\n");
	    				}
	    				break;
	    			};
	    			case IBV_WC_RDMA_READ: {
	    				// finished swapin
	    				request = (req_t*)(void*)wc[i].wr_id;
	    				request->completions++;

	    				if(request->completions == request->issued_sents){
	    //					printf("All pages are swaped-in %p\n",request);
	    					for(rem_t& rem: request->remote_addrs){
	    						all_regions[rem.segment_id]->return_pages(rem.page_id,rem.num_pages);
	    					}	
	    					request->remote_addrs.clear();
	    				}

	    				break;
	    			};    			

	    			default:
	    				exit(1);
	    		}

		    	if(request!=NULL && request->completions == request->issued_sents){
					completed.push_back(request);
					inserted++;
				}
	    	}

	    	return inserted;
	    };

	    void print_stats() override {

	    }	    
	};

