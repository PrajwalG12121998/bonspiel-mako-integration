#include <iostream>
#include <mako.hh>

using namespace std;
using namespace util;

// Static variables needed across functions
static std::atomic<int> end_received(0);
static std::atomic<int> end_received_leader(0);

static void parse_command_line_args(int argc, char **argv, 
                                   string& paxos_proc_name,
                                   int &is_micro,
                                   string& site_name,
                                   vector<string>& paxos_config_file,
                                   int& leader_config)
{
  while (1) {
    static struct option long_options[] =
    {
      {"num-threads"                , required_argument , 0                          , 't'} ,
      {"shard-index"                , required_argument , 0                          , 'g'} ,
      {"shard-config"               , required_argument , 0                          , 'q'} ,
      {"paxos-config"               , required_argument , 0                          , 'F'} ,
      {"paxos-proc-name"            , required_argument , 0                          , 'P'} ,
      {"site-name"                  , required_argument , 0                          , 'N'} ,
      {"is_micro"                   , no_argument       , &is_micro                  ,   1} ,
      {0, 0, 0, 0}
    };
    int option_index = 0;
    int c = getopt_long(argc, argv, "t:g:q:F:P:N:", long_options, &option_index);
    if (c == -1)
      break;

    switch (c) {
    case 0:
      if (long_options[option_index].flag != 0)
        break;
      abort();
      break;

    case 't':
      nthreads = strtoul(optarg, NULL, 10);
      scale_factor = strtod(optarg, NULL);
      ALWAYS_ASSERT(nthreads > 0);
      break;

    case 'g':
      shardIndex = strtoul(optarg, NULL, 10);
      ALWAYS_ASSERT(shardIndex >= 0);
      break;

    case 'N':
      site_name = string(optarg);
      break;

    case 'P':
      paxos_proc_name = string(optarg);
      cluster = paxos_proc_name;
      clusterRole = mako::convertCluster(cluster);
      if (cluster.compare(mako::LOCALHOST_CENTER) == 0) leader_config = 1;
      break;
    
    case 'q':
      config = new transport::Configuration(optarg);
      nshards = config->nshards;
      break;

    case 'F':
      paxos_config_file.push_back(optarg);
      break;

    case '?':
      exit(1);

    default:
      abort();
    }
  }
}

static void handle_new_config_format(const string& site_name,
                                    int& leader_config,
                                    string& paxos_proc_name)
{
  auto site = config->GetSiteByName(site_name);
  if (!site) {
    cerr << "[ERROR] Site " << site_name << " not found in configuration" << endl;
    exit(1);
  }
  
  // Determine if this site is a leader
  leader_config = site->is_leader ? 1 : 0;
  
  // Set shard index from site
  shardIndex = site->shard_id;
  
  // Set cluster role for compatibility
  if (site->is_leader) {
    cluster = mako::LOCALHOST_CENTER;
    clusterRole = mako::LOCALHOST_CENTER_INT;
    paxos_proc_name = mako::LOCALHOST_CENTER;
  } else if (site->replica_idx == 1) {
    cluster = mako::P1_CENTER;
    clusterRole = mako::P1_CENTER_INT;
    paxos_proc_name = mako::P1_CENTER;
  } else if (site->replica_idx == 2) {
    cluster = mako::P2_CENTER;
    clusterRole = mako::P2_CENTER_INT;
    paxos_proc_name = mako::P2_CENTER;
  } else {
    cluster = mako::LEARNER_CENTER;
    clusterRole = mako::LEARNER_CENTER_INT;
    paxos_proc_name = mako::LEARNER_CENTER;
  }
  
  Notice("Site %s: shard=%d, replica_idx=%d, is_leader=%d, cluster=%s", 
         site_name.c_str(), site->shard_id, site->replica_idx, site->is_leader, cluster.c_str());
}

static void print_system_info(const string& paxos_proc_name, size_t numa_memory)
{
  const unsigned long ncpus = coreid::num_cpus_online();
  cerr << "Database Benchmark:"                           << endl;
  cerr << "  pid: " << getpid()                           << endl;
  cerr << "settings:"                                     << endl;
  cerr << "  num-cpus    : " << ncpus                     << endl;
  cerr << "  num-threads : " << nthreads                  << endl;
  cerr << "  shardIndex  : " << shardIndex                << endl;
  cerr << "  paxos_proc_name  : " << paxos_proc_name      << endl;
  cerr << "  nshards     : " << nshards                   << endl;
#ifdef USE_VARINT_ENCODING
  cerr << "  var-encode  : yes"                           << endl;
#else
  cerr << "  var-encode  : no"                            << endl;
#endif

#ifdef USE_JEMALLOC
  cerr << "  allocator   : jemalloc"                      << endl;
#elif defined USE_TCMALLOC
  cerr << "  allocator   : tcmalloc"                      << endl;
#elif defined USE_FLOW
  cerr << "  allocator   : flow"                          << endl;
#else
  cerr << "  allocator   : libc"                          << endl;
#endif
  if (numa_memory > 0) {
    cerr << "  numa-memory : " << numa_memory             << endl;
  } else {
    cerr << "  numa-memory : disabled"                    << endl;
  }

  cerr << "system properties:" << endl;

#ifdef TUPLE_PREFETCH
  cerr << "  tuple_prefetch          : yes" << endl;
#else
  cerr << "  tuple_prefetch          : no" << endl;
#endif

#ifdef BTREE_NODE_PREFETCH
  cerr << "  btree_node_prefetch     : yes" << endl;
#else
  cerr << "  btree_node_prefetch     : no" << endl;
#endif
}

static char** prepare_paxos_args(const vector<string>& paxos_config_file,
                                const string& paxos_proc_name,
                                int kPaxosBatchSize)
{
  int argc_paxos = 18;
  char **argv_paxos = new char*[argc_paxos];
  int k = 0;
  
  argv_paxos[0] = (char *) "";
  argv_paxos[1] = (char *) "-b";
  argv_paxos[2] = (char *) "-d";
  argv_paxos[3] = (char *) "60";
  argv_paxos[4] = (char *) "-f";
  argv_paxos[5] = (char *) paxos_config_file[k++].c_str();
  argv_paxos[6] = (char *) "-f";
  argv_paxos[7] = (char *) paxos_config_file[k++].c_str();
  argv_paxos[8] = (char *) "-t";
  argv_paxos[9] = (char *) "30";
  argv_paxos[10] = (char *) "-T";
  argv_paxos[11] = (char *) "100000";
  argv_paxos[12] = (char *) "-n";
  argv_paxos[13] = (char *) "32";
  argv_paxos[14] = (char *) "-P";
  argv_paxos[15] = (char *) paxos_proc_name.c_str();
  argv_paxos[16] = (char *) "-A";
  argv_paxos[17] = new char[20];
  memset(argv_paxos[17], '\0', 20);
  sprintf(argv_paxos[17], "%d", kPaxosBatchSize);
  
  return argv_paxos;
}

static RustWrapper* initialize_rust_wrapper()
{
  RustWrapper* g_rust_wrapper = new RustWrapper();
  if (!g_rust_wrapper->init()) {
      std::cerr << "Failed to initialize rust wrapper!" << std::endl;
      delete g_rust_wrapper;
      return nullptr;
  }
  std::cout << "Successfully initialized rust wrapper!" << std::endl;
  return g_rust_wrapper;
}

static void setup_sync_util_callbacks()
{
  // Invoke get_epoch function
  register_sync_util([&]() {
#if defined(PAXOS_LIB_ENABLED)
     return get_epoch();
#else
    return 0;
#endif
  });

  // rpc client
  register_sync_util_sc([&]() {
#if defined(FAIL_NEW_VERSION)
     return get_epoch();
#else
    return 0;
#endif
  });

  // rpc server
  register_sync_util_ss([&]() {
#if defined(FAIL_NEW_VERSION)
     return get_epoch();
#else
    return 0;
#endif
  });
}

static void setup_transport_callbacks()
{
  // happens on the elected follower-p1, to be the new leader datacenter
  register_fasttransport_for_dbtest([&](int control, int value) {
    Warning("receive a control in register_fasttransport_for_dbtest: %d", control);
    switch (control) {
      case 4: {
        // 1. stop the exchange server on p1 datacenter
        // 2. increase the epoch
        // 3. add no-ops
        // 4. sync the logs
        // 5. start the worker threads 
        // change the membership
        upgrade_p1_to_leader();

        string log = "no-ops:" + to_string(get_epoch());
        for(int i = 0; i < nthreads; i++){
          add_log_to_nc(log.c_str(), log.size(), i);
        }

        // start the worker threads
        std::lock_guard<std::mutex> lk((sync_util::sync_logger::m));
        sync_util::sync_logger::toLeader = true ;
        std::cout << "notify a new leader is elected!\n" ;
        //sync_util::sync_logger::worker_running = true;
        sync_util::sync_logger::cv.notify_one();

        // terminate the exchange-watermark server
        sync_util::sync_logger::exchange_running = false;
        break;
      }
    }
    return 0;
  });
}

static void setup_leader_election_callback()
{
  register_leader_election_callback([&](int control) { // communicate with third party: Paxos
    // happens on the learner for case 0 and case 2, 3
    uint32_t aa = mako::getCurrentTimeMillis();
    Warning("Receive a control command:%d, current ms: %llu", control, aa);
    switch (control) {
#if defined(FAIL_NEW_VERSION)
      case 0: {
        std::cout<<"Implement a new fail recovery!"<<std::endl;
        sync_util::sync_logger::exchange_running = false;
        sync_util::sync_logger::failed_shard_index = shardIndex;
        sync_util::sync_logger::client_control(0, shardIndex); // in bench.cc register_fasttransport_for_bench
        break;
      }
      case 2: {
        // Wait for FVW in the old epoch (w * 10 + epoch); this is very important in our new implementation
        // Single timestamp system: collect all shard watermarks and use maximum
        uint32_t max_watermark = 0;
        for (int i=0; i<nshards; i++) {
          int clusterRoleLocal = mako::LOCALHOST_CENTER_INT;
          if (i==0) 
            clusterRoleLocal = mako::LEARNER_CENTER_INT;
          mako::NFSSync::wait_for_key("fvw_"+std::to_string(i), 
                                          config->shard(0, clusterRoleLocal).host.c_str(), config->mports[clusterRole]);
          std::string w_i = mako::NFSSync::get_key("fvw_"+std::to_string(i), 
                                                      config->shard(0, clusterRoleLocal).host.c_str(), 
                                                      config->mports[clusterRoleLocal]);
          std::cout<<"get fvw, " << clusterRoleLocal << ", fvw_"+std::to_string(i)<<":"<<w_i<<std::endl;
          uint32_t watermark = std::stoi(w_i);
          max_watermark = std::max(max_watermark, watermark);
        }

        sync_util::sync_logger::update_stable_timestamp(get_epoch()-1, max_watermark);
        sync_util::sync_logger::client_control(1, shardIndex);

        // Start transactions in new epoch
        std::lock_guard<std::mutex> lk((sync_util::sync_logger::m));
        sync_util::sync_logger::toLeader = true ;
        std::cout << "notify a new leader is elected!\n" ;
        //sync_util::sync_logger::worker_running = true;
        sync_util::sync_logger::cv.notify_one();
        auto x0 = std::chrono::high_resolution_clock::now() ;
        break;
      }
#else
      // for the partial datacenter failure
      case 0: {
        // 0. stop exchange client + server on the new leader (learner)
        sync_util::sync_logger::exchange_running = false;
        // 1. issue a control command to all other leader partition servers to
        //    1.1 pause other servers DB threads 
        //    1.2 config update 
        //    1.3 issue no-ops within the old epoch
        //    1.4 start the controller
        sync_util::sync_logger::failed_shard_index = shardIndex;

        auto x0 = std::chrono::high_resolution_clock::now() ;
        sync_util::sync_logger::client_control(0, shardIndex); // in bench.cc register_fasttransport_for_bench
        auto x1 = std::chrono::high_resolution_clock::now() ;
        printf("first connection:%d\n",
            std::chrono::duration_cast<std::chrono::microseconds>(x1-x0).count());
        break;
      }
      case 2: {// notify that you're the new leader; PREPARE
         sync_util::sync_logger::client_control(1, shardIndex);
         // wait for Paxos logs replicated
         auto x0 = std::chrono::high_resolution_clock::now() ;
         WAN_WAIT_TIME;
         auto x1 = std::chrono::high_resolution_clock::now() ;
         printf("replicated:%d\n",
            std::chrono::duration_cast<std::chrono::microseconds>(x1-x0).count());
         break;
      }
      case 3: {  // COMMIT
        std::lock_guard<std::mutex> lk((sync_util::sync_logger::m));
        sync_util::sync_logger::toLeader = true ;
        std::cout << "notify a new leader is elected!\n" ;
        //sync_util::sync_logger::worker_running = true;
        sync_util::sync_logger::cv.notify_one();
        auto x0 = std::chrono::high_resolution_clock::now() ;
        sync_util::sync_logger::client_control(2, shardIndex);
        auto x1 = std::chrono::high_resolution_clock::now() ;
        printf("second connection:%d\n",
            std::chrono::duration_cast<std::chrono::microseconds>(x1-x0).count());
        break;
      }
      // for the datacenter failure, triggered on p1
      case 4: {
        // send a message to all p1 follower nodes within the same datacenter
        sync_util::sync_logger::client_control(4, shardIndex);
        break;
      }
#endif
    }
  });
}

static void wait_for_termination()
{
  bool isLearner = cluster.compare(mako::LEARNER_CENTER)==0 ;
  // in case, the Paxos streams on other side is terminated, 
  // not need for all no-ops for the final termination
  while (!(end_received.load() > 0 ||end_received_leader.load() > 0)) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    if (isLearner)
      Notice("learner is waiting for being ended: %d/%zu, noops_cnt:%d\n", end_received.load(), nthreads, sync_util::sync_logger::noops_cnt.load());
    else
      Notice("follower is waiting for being ended: %d/%zu, noops_cnt:%d\n", end_received.load(), nthreads, sync_util::sync_logger::noops_cnt.load());
    //if (end_received.load() > 0) {std::quick_exit( EXIT_SUCCESS );}
  }
}

static void register_paxos_follower_callback(TSharedThreadPoolMbta& tpool_mbta, int thread_id)
{
#if defined(PAXOS_LIB_ENABLED)
  //transport::ShardAddress addr = config->shard(shardIndex, mako::LEARNER_CENTER);
  register_for_follower_par_id_return([&,thread_id](const char*& log, int len, int par_id, int slot_id, std::queue<std::tuple<int, int, int, int, const char *>> & un_replay_logs_) {
    //Warning("receive a register_for_follower_par_id_return, par_id:%d, slot_id:%d,len:%d",par_id, slot_id,len);
    int status = mako::PaxosStatus::STATUS_INIT;
    uint32_t timestamp = 0;  // Track timestamp for return value encoding
    abstract_db * db = tpool_mbta.getDBWrapper(par_id)->getDB () ;
    bool noops = false;

    if (len==mako::ADVANCER_MARKER_NUM) { // start a advancer
      status = mako::PaxosStatus::STATUS_REPLAY_DONE;
      if (par_id==0){
        std::cout << "we can start a advancer" << std::endl;
        sync_util::sync_logger::start_advancer();
      }
      return status; 
    }

    // ending of Paxos group
    if (len==0) {
      Warning("Recieved a zero length log");
      status = mako::PaxosStatus::STATUS_ENDING;
      // update the timestamp for this Paxos stream so that not blocking other Paxos streams
      uint32_t min_so_far = numeric_limits<uint32_t>::max();
      sync_util::sync_logger::local_timestamp_[par_id].store(min_so_far, memory_order_release) ;
      end_received+=1;
    }

    // deal with Paxos log
    if (len>0) {
      if (isNoops(log,len)!=-1) {
        Warning("receive a noops, par_id:%d on follower_callback_,%s",par_id,log);
        if (par_id==0) set_epoch(isNoops(log,len));
        noops=true;
        status = mako::PaxosStatus::STATUS_NOOPS;
      }

      if (noops) {
        sync_util::sync_logger::noops_cnt++;
        while (1) { // check if all threads receive noops
          if (sync_util::sync_logger::noops_cnt.load(memory_order_acquire)==nthreads) {
            break ;
          }
          sleep(0);
          break;
        }
        Warning("phase-1,par_id:%d DONE",par_id);
        if (par_id==0) {
          uint32_t local_w = sync_util::sync_logger::computeLocal();
          //Warning("update %s in phase-1 on port:%d", ("noops_phase_"+std::to_string(shardIndex)).c_str(), config->mports[clusterRole]);
          mako::NFSSync::set_key("noops_phase_"+std::to_string(shardIndex), 
                                     std::to_string(local_w).c_str(),
                                     config->shard(0, clusterRole).host.c_str(),
                                     config->mports[clusterRole]);


          //Warning("We update local_watermark[%d]=%llu",shardIndex, local_w);
          // update the history stable timestamp
          // TODO: replay inside the function
          sync_util::sync_logger::update_stable_timestamp(get_epoch()-1, sync_util::sync_logger::retrieveShardW()/10);
        }
      }else{
        CommitInfo commit_info = get_latest_commit_info((char *) log, len);
        timestamp = commit_info.timestamp;  // Store for return value encoding
        sync_util::sync_logger::local_timestamp_[par_id].store(commit_info.timestamp, memory_order_release) ;
        uint32_t w = sync_util::sync_logger::retrieveW();
        // Single timestamp safety check
        if (sync_util::sync_logger::safety_check(commit_info.timestamp, w)) { // pass safety check
          treplay_in_same_thread_opt_mbta_v2(par_id, (char*)log, len, db, nshards);
          //Warning("replay[YES] par_id:%d,st:%u,slot_id:%d,un_replay_logs_:%d", par_id, commit_info.timestamp, slot_id,un_replay_logs_.size());
          status = mako::PaxosStatus::STATUS_REPLAY_DONE;
        } else {
          //Warning("replay[NO] par_id:%d,st:%u,slot_id:%d,un_replay_logs_:%d", par_id, commit_info.timestamp, slot_id,un_replay_logs_.size());
          status = mako::PaxosStatus::STATUS_SAFETY_FAIL;
        }
      }
    }

    // wait for vectorized watermark computed from other partition servers
    if (noops) {
      for (int i=0; i<nshards; i++) {
        if (i!=shardIndex && par_id==0) {
          mako::NFSSync::wait_for_key("noops_phase_"+std::to_string(i), 
                                          config->shard(0, clusterRole).host.c_str(), config->mports[clusterRole]);
          std::string local_w = mako::NFSSync::get_key("noops_phase_"+std::to_string(i), 
                                                          config->shard(0, clusterRole).host.c_str(), 
                                                          config->mports[clusterRole]);

          //Warning("We update local_watermark[%d]=%s (others)",i, local_w.c_str());
          // In single timestamp system, use max value from all shards
          uint32_t new_watermark = std::stoull(local_w);
          uint32_t current = sync_util::sync_logger::single_watermark_.load(memory_order_acquire);
          if (new_watermark > current) {
              sync_util::sync_logger::single_watermark_.store(new_watermark, memory_order_release);
          }
        }
      }
    }
    auto w = sync_util::sync_logger::retrieveW(); 

    while (un_replay_logs_.size() > 0) {
        auto it = un_replay_logs_.front() ;
        if (sync_util::sync_logger::safety_check(std::get<0>(it), w)) {
          //Warning("replay-2 par_id:%d, slot_id:%d,un_replay_logs_:%d", par_id, std::get<1>(it),un_replay_logs_.size());
          auto nums = treplay_in_same_thread_opt_mbta_v2(par_id, (char *) std::get<4>(it), std::get<3>(it), db, nshards);
          un_replay_logs_.pop() ;
          free((char*)std::get<4>(it));
        } else {
          if (noops){
            un_replay_logs_.pop() ; // TODOs: should compare each transactions one by one
            Warning("no-ops pop a log, par_id:%d,slot_id:%d", par_id,std::get<1>(it));
            free((char*)std::get<4>(it));
          }else{
            break ;
          }
        }
    }

    // wait for all worker threads replay DONE
    if (noops){
      sync_util::sync_logger::noops_cnt_hole++ ;
      while (1) {
        if (sync_util::sync_logger::noops_cnt_hole.load(memory_order_acquire)==nthreads) {
          break ;
        } else {
          sleep(0);
          break;
        }
      }

      Warning("phase-3,par_id:%d DONE",par_id);

      if (par_id==0) {
        sync_util::sync_logger::reset();
      }
    }
    // Return timestamp * 10 + status (for safety check compatibility)
    return static_cast<int>(timestamp * 10 + status);
  }, 
  thread_id);
#endif
}

static void register_paxos_leader_callback(vector<pair<uint32_t, uint32_t>>& advanceWatermarkTracker, int thread_id)
{
#if defined(PAXOS_LIB_ENABLED)
  register_for_leader_par_id_return([&,thread_id](const char*& log, int len, int par_id, int slot_id, std::queue<std::tuple<int, int, int, int, const char *>> & un_replay_logs_) {
    //Warning("receive a register_for_leader_par_id_return, par_id:%d, slot_id:%d,len:%d",par_id, slot_id,len);
    int status = mako::PaxosStatus::STATUS_NORMAL;
    uint32_t timestamp = 0;  // Track timestamp for return value encoding
    bool noops = false;

    if (len==mako::ADVANCER_MARKER_NUM) { // start a advancer
      status = mako::PaxosStatus::STATUS_REPLAY_DONE;
      if (par_id==0){
        std::cout << "we can start a advancer" << std::endl;
        sync_util::sync_logger::start_advancer();
      }
      return status; 
    }

    if (len==0) {
      status = mako::PaxosStatus::STATUS_ENDING;
      Warning("Recieved a zero length log");
      uint32_t min_so_far = numeric_limits<uint32_t>::max();
      sync_util::sync_logger::local_timestamp_[par_id].store(min_so_far, memory_order_release) ;
      end_received_leader++;
    }

    if (len>0) {
      if (isNoops(log,len)!=-1) {
        //Warning("receive a noops, par_id:%d on leader_callback_,log:%s",par_id,log);
        noops=true;
        status = mako::PaxosStatus::STATUS_NOOPS;
      }

      if (noops) {
        sync_util::sync_logger::noops_cnt++;
        while (1) { // check if all threads receive noops
          if (sync_util::sync_logger::noops_cnt.load(memory_order_acquire)==nthreads) {
            break ;
          }
          sleep(0);
          break;
        }
        Warning("phase-1,par_id:%d DONE",par_id);
        if (par_id==0) {
          uint32_t local_w = sync_util::sync_logger::computeLocal();
          mako::NFSSync::set_key("noops_phase_"+std::to_string(shardIndex), 
                                        std::to_string(local_w).c_str(),
                                        config->shard(0, clusterRole).host.c_str(), 
                                        config->mports[clusterRole]);
          sync_util::sync_logger::update_stable_timestamp(get_epoch()-1, sync_util::sync_logger::retrieveShardW()/10);
#if defined(FAIL_NEW_VERSION)
          mako::NFSSync::set_key("fvw_"+std::to_string(shardIndex), 
                                     std::to_string(local_w).c_str(),
                                     config->shard(0, clusterRole).host.c_str(),
                                     config->mports[clusterRole]);
          std::cout<<"set fvw, " << clusterRole << ", fvw_"+std::to_string(shardIndex)<<":"<<local_w<<std::endl;
#endif
          sync_util::sync_logger::reset(); 
        }
      }else {
        CommitInfo commit_info = get_latest_commit_info((char *) log, len);
        timestamp = commit_info.timestamp;  // Store for return value encoding
        
        uint32_t end_time = mako::getCurrentTimeMillis();
        //Warning("In register_for_leader_par_id_return, par_id:%d, slot_id:%d, len:%d, st: %llu, et: %llu, latency: %llu",
        //       par_id, slot_id, len, commit_info.latency_tracker, end_time, end_time-commit_info.latency_tracker);
        sync_util::sync_logger::local_timestamp_[par_id].store(commit_info.timestamp, memory_order_release) ;

#if defined(TRACKING_LATENCY)
        if (par_id==4){
          uint32_t vw = sync_util::sync_logger::computeLocal();
          //Warning("Update here: %llu, before:%llu",vw/10,vw);
          advanceWatermarkTracker.push_back(std::make_pair(vw/10 /* actual watermark */, mako::getCurrentTimeMillis()));
        }
#endif
      }
    }
    // Return timestamp * 10 + status (for safety check compatibility)
    return static_cast<int>(timestamp * 10 + status);
  },
  thread_id);
#endif
}

static void setup_paxos_callbacks(TSharedThreadPoolMbta& tpool_mbta, 
                                  vector<pair<uint32_t, uint32_t>>& advanceWatermarkTracker)
{
#if defined(PAXOS_LIB_ENABLED)
  for (int i = 0; i < nthreads; i++) {
    register_paxos_follower_callback(tpool_mbta, i);
    register_paxos_leader_callback(advanceWatermarkTracker, i);
  }
#endif
}

static void run_latency_tracking(int leader_config,
                                const vector<pair<uint32_t, uint32_t>>& advanceWatermarkTracker)
{
#if defined(TRACKING_LATENCY)
  if(leader_config) {
        uint32_t latency_ts = 0;
        std::map<uint32_t, uint32_t> ordered(sample_transaction_tracker.begin(),
                                                           sample_transaction_tracker.end());
        int valid_cnt = 0;
        std::vector<float> latencyVector ;
        for (auto it: ordered) {  // cid => updated time
            int i = 0;
            for (; i < advanceWatermarkTracker.size(); i++) { // G => updated time
                if (advanceWatermarkTracker[i].first >= it.first) break;
            }
            if (i < advanceWatermarkTracker.size() && advanceWatermarkTracker[i].first >= it.first) {
                latency_ts += advanceWatermarkTracker[i].second - it.second;
                latencyVector.emplace_back(advanceWatermarkTracker[i].second - it.second) ;
                valid_cnt++;
                // std::cout << "Transaction: " << it.first << " takes "
                //          << (advanceWatermarkTracker[i].second - it.second) << " ms" 
                //          << ", end_time: " << advanceWatermarkTracker[i].second 
                //          << ", st_time: " << it.second << std::endl;
            } else { // incur only for last several transactions

            }
        }
        if (latencyVector.size() > 0) {
            std::cout << "averaged latency: " << latency_ts / valid_cnt << std::endl;
            std::sort (latencyVector.begin(), latencyVector.end());
            std::cout << "10% latency: " << latencyVector[(int)(valid_cnt *0.1)]  << std::endl;
            std::cout << "50% latency: " << latencyVector[(int)(valid_cnt *0.5)]  << std::endl;
            std::cout << "90% latency: " << latencyVector[(int)(valid_cnt *0.9)]  << std::endl;
            std::cout << "95% latency: " << latencyVector[(int)(valid_cnt *0.95)]  << std::endl;
            std::cout << "99% latency: " << latencyVector[(int)(valid_cnt *0.99)]  << std::endl;
        }
  }
#endif
}

static void run_workers(int leader_config, abstract_db* db, TSharedThreadPoolMbta& tpool_mbta)
{
  if (leader_config) { // leader cluster
    bench_runner *r = start_workers_tpcc(leader_config, db, nthreads);
    start_workers_tpcc(leader_config, db, nthreads, false, 1, r);
    delete db;
  } else if (cluster.compare(mako::LEARNER_CENTER)==0) { // learner cluster
    abstract_db * db = tpool_mbta.getDBWrapper(nthreads)->getDB () ;
    bench_runner *r = start_workers_tpcc(1, db, nthreads, true);
    modeMonitor(db, nthreads, r) ;
  }
}

static void cleanup_and_shutdown()
{
#if defined(PAXOS_LIB_ENABLED)
  std::this_thread::sleep_for(2s);
  pre_shutdown_step();
  shutdown_paxos();
#endif

  sync_util::sync_logger::shutdown();
  std::quick_exit( EXIT_SUCCESS );
}

int
main(int argc, char **argv)
{
  abstract_db *db = NULL;
  string basedir = "./tmp";
  size_t numa_memory = mako::parse_memory_spec("1G");
  string bench_opts = "--new-order-fast-id-gen";

  int leader_config = 0;
  int kPaxosBatchSize = 50000;
  vector<string> paxos_config_file{};
  string paxos_proc_name = mako::LOCALHOST_CENTER;
  string site_name = "";  // For new config format

  // tracking vectorized watermark
  std::vector<std::pair<uint32_t, uint32_t>> advanceWatermarkTracker;  // local watermark -> time

  // Parse command line arguments
  parse_command_line_args(argc, argv, paxos_proc_name, is_micro, site_name, paxos_config_file, leader_config);

  // Handle new configuration format if site name is provided
  if (!site_name.empty() && config != nullptr) {
    handle_new_config_format(site_name, leader_config, paxos_proc_name);
  }


  // initialize the numa allocator
  if (numa_memory > 0) {
    const size_t maxpercpu = util::iceil(
        numa_memory / nthreads, ::allocator::GetHugepageSize());
    numa_memory = maxpercpu * nthreads;
    ::allocator::Initialize(nthreads, maxpercpu);
  }

  db = new mbta_wrapper; // on the leader replica

  // Print system information
  print_system_info(paxos_proc_name, numa_memory);

  sync_util::sync_logger::Init(shardIndex, nshards, nthreads, 
                               leader_config==1, /* is leader */ 
                               cluster,
                               config);
  
  // Prepare Paxos arguments
  if (paxos_config_file.size() < 2) {
      cerr << "no enough paxos config files" << endl;
      return 1;
  }
  char** argv_paxos = prepare_paxos_args(paxos_config_file, paxos_proc_name, kPaxosBatchSize);

  // Initialize Rust wrapper
  RustWrapper* g_rust_wrapper = initialize_rust_wrapper();
  if (!g_rust_wrapper) {
      return 1;
  }
  g_rust_wrapper->start_polling();
    
  TSharedThreadPoolMbta tpool_mbta (nthreads+1);
  if (!leader_config) { // initialize tables on follower replicas
    abstract_db * db = tpool_mbta.getDBWrapper(nthreads)->getDB () ;
    // pre-initialize all tables to avoid table creation data race
    for (int i=0;i<((size_t)scale_factor)*11+1;i++) {
      db->open_index(i+1);
    }
  }

  // Setup callbacks
  setup_sync_util_callbacks();
  setup_transport_callbacks();
  setup_leader_election_callback();

#if defined(PAXOS_LIB_ENABLED)
  //StringAllocator::setSTOBatchSize(kSTOBatchSize);
 
  std::vector<std::string> ret = setup(18, argv_paxos);
  if (ret.empty()) {
    return -1;
  }

  // Setup Paxos callbacks - MUST be after setup() is called
  setup_paxos_callbacks(tpool_mbta, advanceWatermarkTracker);

  int ret2 = setup2(0, shardIndex);
  sleep(3); // ensure that all get started
#endif // END OF PAXOS_LIB_ENABLED

  // Run worker threads
  run_workers(leader_config, db, tpool_mbta);

  // Track and report latency if configured
  run_latency_tracking(leader_config, advanceWatermarkTracker);

  // Wait for termination if not a leader
  if (!leader_config) {
    wait_for_termination();
  }

  // Cleanup and shutdown
  cleanup_and_shutdown();

  return 0;
}
