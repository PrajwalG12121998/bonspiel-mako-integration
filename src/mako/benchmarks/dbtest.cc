#include <iostream>
#include <mako.hh>

using namespace std;
using namespace util;


static void parse_command_line_args(int argc, 
                                    char **argv, 
                                    int &is_micro,
                                    int &is_replicated,
                                    string& site_name,
                                    vector<string>& paxos_config_file)
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
      {"is-micro"                   , no_argument       , &is_micro                  ,   1} ,
      {"is-replicated"              , no_argument       , &is_replicated             ,   1} ,
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

    case 't': {
      auto& config = BenchmarkConfig::getInstance();
      config.setNthreads(strtoul(optarg, NULL, 10));
      ALWAYS_ASSERT(config.getNthreads() > 0);
      }
      break;

    case 'g': {
      auto& config = BenchmarkConfig::getInstance();
      config.setShardIndex(strtoul(optarg, NULL, 10));
      ALWAYS_ASSERT(config.getShardIndex() >= 0);
      }
      break;

    case 'N':
      site_name = string(optarg);
      break;

    case 'P': {
      auto& config = BenchmarkConfig::getInstance();
      config.setPaxosProcName(string(optarg));
      }
      break;
    
    case 'q': {
      auto& benchConfig = BenchmarkConfig::getInstance();
      transport::Configuration* transportConfig = new transport::Configuration(optarg);
      benchConfig.setConfig(transportConfig);
      benchConfig.setNshards(transportConfig->nshards);
      }
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

static void handle_new_config_format(const string& site_name)
{
  auto& benchConfig = BenchmarkConfig::getInstance();
  auto site = benchConfig.getConfig()->GetSiteByName(site_name);
  if (!site) {
    cerr << "[ERROR] Site " << site_name << " not found in configuration" << endl;
    exit(1);
  }
  
  // Set shard index from site
  benchConfig.setShardIndex(site->shard_id);
  
  // Set cluster role for compatibility
  if (site->is_leader) {
    benchConfig.setPaxosProcName(mako::LOCALHOST_CENTER);
  } else if (site->replica_idx == 1) {
    benchConfig.setPaxosProcName(mako::P1_CENTER);
  } else if (site->replica_idx == 2) {
    benchConfig.setPaxosProcName(mako::P2_CENTER);
  } else {
    benchConfig.setPaxosProcName(mako::LEARNER_CENTER);
  }
  
  Notice("Site %s: shard=%d, replica_idx=%d, is_leader=%d, cluster=%s", 
         site_name.c_str(), site->shard_id, site->replica_idx, site->is_leader, benchConfig.getCluster().c_str());
}

static void run_workers(abstract_db* db)
{
  auto& benchConfig = BenchmarkConfig::getInstance();
  bench_runner *r = start_workers_tpcc(benchConfig.getLeaderConfig(), db, benchConfig.getNthreads());
  start_workers_tpcc(benchConfig.getLeaderConfig(), db, benchConfig.getNthreads(), false, 1, r);
  delete db;
}

int
main(int argc, char **argv)
{

  // Parameters prepared
  int is_micro = 0;  // Flag for micro benchmark mode
  int is_replicated = 0;  // if use Paxos to replicate
  vector<string> paxos_config_file{};
  string site_name = "";  // For new config format

  auto& benchConfig = BenchmarkConfig::getInstance();
  // Parse command line arguments
  parse_command_line_args(argc, argv, is_micro, is_replicated, site_name, paxos_config_file);

  // Handle new configuration format if site name is provided
  if (!site_name.empty() && benchConfig.getConfig() != nullptr) {
    handle_new_config_format(site_name);
  }

  benchConfig.setIsMicro(is_micro);
  benchConfig.setIsReplicated(is_replicated);
  benchConfig.setPaxosConfigFile(paxos_config_file);
  
  // Setup callbacks
  setup_sync_util_callbacks();

  if (benchConfig.getIsReplicated())  { // failures handling callbacks
    setup_transport_callbacks();
    setup_leader_election_callbacks();
  }

  // tpool_mbta is db-instance wrapper if replicated is enabled && on followers/learners
  TSharedThreadPoolMbta tpool_mbta (benchConfig.getNthreads()+1);
  if (!benchConfig.getLeaderConfig()) {
    abstract_db * db = tpool_mbta.getDBWrapper(benchConfig.getNthreads())->getDB () ;
    // pre-initialize all tables to avoid table creation data race
    for (int i=0;i<((size_t)benchConfig.getScaleFactor())*11+1;i++) {
      db->open_index(i+1);
    }
  }

  if (BenchmarkConfig::getInstance().getIsReplicated()) {
    char** argv_paxos = prepare_paxos_args(benchConfig.getPaxosConfigFile(), benchConfig.getPaxosProcName());
    std::vector<std::string> ret = setup(18, argv_paxos);
    if (ret.empty()) {
      return -1;
    }

    // Setup Paxos callbacks have to be after setup() is called
    setup_paxos_leader_callbacks(benchConfig.getAdvanceWatermarkTracker());
    setup_paxos_follower_callbacks(tpool_mbta);

    int ret2 = setup2(0, benchConfig.getShardIndex());
    sleep(3); // ensure that all get started

    // start a monitor on learner
    if (benchConfig.getCluster().compare(mako::LEARNER_CENTER)==0) { // learner cluster
      abstract_db * db = tpool_mbta.getDBWrapper(benchConfig.getNthreads())->getDB () ;
      bench_runner *r = start_workers_tpcc(1, db, benchConfig.getNthreads(), true);
      modeMonitor(db, benchConfig.getNthreads(), r) ;
    }
  }

  abstract_db * db = initWithDB();
  // Run worker threads on the leader
  if (benchConfig.getLeaderConfig()) {
    run_workers(db);
  }

  // Wait for termination if not a leader
  if (!benchConfig.getLeaderConfig()) {
    wait_for_termination();
  }

  // Cleanup and shutdown
  cleanup_and_shutdown();

  return 0;
}
