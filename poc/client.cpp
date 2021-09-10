#include <iostream>       // std::cout
#include <infiniband/verbs.h>
#include <chrono>
#include "cxxopts.hpp"
#include  "ClientRDMA.hpp"
#include  "VerbsEP.hpp"
#include  <unordered_map>


#include "swap.hpp"

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
      ("w,warmup", "warmup iterations", cxxopts::value<uint32_t>()->default_value("1024"), "N")
      ("t,test", "test iterations", cxxopts::value<uint32_t>()->default_value("1024"), "N")
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

void latency_test(Swap& s,char* addr, uint8_t pages,uint32_t warmup, uint32_t test, bool cold_test){
  std::deque<req_t*> completed;
  using namespace std::chrono;
  struct req_t req;

  req.sent_pages = 0;
  req.issued_sents = 0;
  req.completions = 0;
  req.num_pages = pages;
  req.local_addr = (uint64_t)addr;

  for(uint32_t i=0; i<warmup; i++){
    s.swapin(&req);
    while(s.poll(completed) == 0){

    }
    completed.clear();
    s.swapout(&req);
    while(s.poll(completed) == 0){

    }
    if(!cold_test){
      auto info = s.meminfo();
      s.evict(info.second);  
    }

  }
  
  std::vector<float> latency_in; 
  std::vector<float> latency_out;
  latency_in.reserve(test);
  latency_out.reserve(test);


  for(uint32_t i=0; i<test; i++){
    auto t1 = high_resolution_clock::now();
    s.swapin(&req);
    while(s.poll(completed) == 0){

    }
    auto t2 = high_resolution_clock::now();
    completed.clear();
    s.swapout(&req);
    
    while(s.poll(completed) == 0){

    }
    auto t3 = high_resolution_clock::now();

    if(!cold_test){
      auto info = s.meminfo();
      s.evict(info.second);  
    }
    float lat_swapin = duration_cast<nanoseconds>(t2 - t1).count()/ 1000.0;
    float lat_swapout = duration_cast<nanoseconds>(t3 - t2).count()/ 1000.0;
    latency_in.push_back(lat_swapin);    
    latency_out.push_back(lat_swapout);    
  }
  auto info = s.meminfo();
  if(info.second) s.evict(info.second);  
  
  std::sort (latency_in.begin(), latency_in.end());
  int q025 = (int)(test*0.025);
  int q050 = (int)(test*0.05);
  int q500 = (int)(test*0.5);
  int q950 = (int)(test*0.950);
  int q975 = (int)(test*0.975);
  printf("latency_in {%f-%f-%f-%f-%f} us ",  latency_in[q025], latency_in[q050], latency_in[q500],  latency_in[q950], latency_in[q975]);
  
  std::sort (latency_out.begin(), latency_out.end());
  printf("latency_out {%f-%f-%f-%f-%f} us ",  latency_out[q025], latency_out[q050], latency_out[q500],  latency_out[q950], latency_out[q975]);
  printf("\n");
}


void spike_test(Swap& s, char* addr, uint8_t pages, uint16_t spike_size, uint32_t warmup, uint32_t test, bool cold_test){
  std::deque<req_t*> completed;
  using namespace std::chrono;
  std::vector<struct req_t> requests;
  requests.resize(spike_size);


  for(uint32_t i=0; i<requests.size(); i++){
    requests[i].sent_pages = 0;
    requests[i].issued_sents = 0;
    requests[i].completions = 0;
    requests[i].num_pages = pages;
    requests[i].local_addr = (uint64_t)addr + (4096UL * pages) * i;
  }

  for(uint32_t i=0; i<warmup; i++){
    for(uint32_t j=0; j<requests.size(); j++){
      s.swapin(&requests[j]);
    }
    while(completed.size()<requests.size()){
      s.poll(completed);
    }
    completed.clear();
    
    for(uint32_t j=0; j<requests.size(); j++){
      s.swapout(&requests[j]);
    }

    while(completed.size()<requests.size()){
      s.poll(completed);
    }
    completed.clear();

    if(!cold_test){
      auto info = s.meminfo();
      s.evict(info.second);  
    }

  }
  
  std::vector<float> latency_in; 
  std::vector<float> latency_out;
  latency_in.reserve(test);
  latency_out.reserve(test);


  for(uint32_t i=0; i<test; i++){
    auto t1 = high_resolution_clock::now();
    for(uint32_t j=0; j<requests.size(); j++){
      s.swapin(&requests[j]);
    }
    while(completed.size()<requests.size()){
      s.poll(completed);
    }
    auto t2 = high_resolution_clock::now();
    completed.clear();
    
    for(uint32_t j=0; j<requests.size(); j++){
      s.swapout(&requests[j]);
    }

    while(completed.size()<requests.size()){
      s.poll(completed);
    }
    auto t3 = high_resolution_clock::now();
    completed.clear();

    if(!cold_test){
      auto info = s.meminfo();
      s.evict(info.second);  
    }
    
    float lat_swapin = duration_cast<nanoseconds>(t2 - t1).count()/ 1000.0;
    float lat_swapout = duration_cast<nanoseconds>(t3 - t2).count()/ 1000.0;
    latency_in.push_back(lat_swapin);    
    latency_out.push_back(lat_swapout);    
  }

  auto info = s.meminfo();
  if(info.second) s.evict(info.second);  
  
  std::sort (latency_in.begin(), latency_in.end());
  int q025 = (int)(test*0.025);
  int q050 = (int)(test*0.05);
  int q500 = (int)(test*0.5);
  int q950 = (int)(test*0.950);
  int q975 = (int)(test*0.975);
  printf("spike_in {%f-%f-%f-%f-%f} us ",  latency_in[q025], latency_in[q050], latency_in[q500],  latency_in[q950], latency_in[q975]);
  
  std::sort (latency_out.begin(), latency_out.end());
  printf("spike_out {%f-%f-%f-%f-%f} us ",  latency_out[q025], latency_out[q050], latency_out[q500],  latency_out[q950], latency_out[q975]);
  printf("\n");
}



int main(int argc, char* argv[]){
// parse the arguments and creates a dictionary which maps arguments to values
    auto allparams = parse(argc,argv);

    InfiniSwap s1;
    FiniteSwap s2;

    std::string ip = allparams["address"].as<std::string>(); // "192.168.1.20"; .c_str()
    int port = 9999;


    uint32_t warmup = allparams["warmup"].as<uint32_t>(); 
    uint32_t test = allparams["test"].as<uint32_t>(); 
    
    char* mem = (char*)aligned_alloc(4096,4096*16);  // 16 pages
    if(allparams.count("infiniswap") ){
        s1.connect(ip,port);
    }else{
        s2.connect(ip,port);
    }
    
    struct ibv_pd* pd = allparams.count("infiniswap") ? s1.pd : s2.pd ;   
    struct ibv_mr* mr = ibv_reg_mr(pd, mem, 16*4096,  IBV_ACCESS_LOCAL_WRITE);
    
    if(allparams.count("infiniswap")){
      s1.install_memory(mr->lkey);
    }else{
      s2.install_memory(mr->lkey);
    }

    for(uint32_t i = 1; i<=16; i++){
      printf("start test with %u page\n",i);
      if(allparams.count("infiniswap")){
        latency_test(s1,mem,i,warmup,test,false);
        latency_test(s1,mem,i,warmup,test,true);
      }else{
        latency_test(s2,mem,i,warmup,test,false);
        latency_test(s2,mem,i,warmup,test,true);
      }
    }

    for(uint32_t i = 1; i<=16; i=i*2){
      printf("start spike test with %u pages and spike size %u \n",i,16/i);
      if(allparams.count("infiniswap")){
        spike_test(s1,mem,i,16/i,warmup,test,false);
        spike_test(s1,mem,i,16/i,warmup,test,true);
      }else{
        spike_test(s2,mem,i,16/i,warmup,test,false);
        spike_test(s2,mem,i,16/i,warmup,test,true);
      }
    }



    
}
 
