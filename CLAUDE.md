# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository contains two related distributed transaction systems:
- **Janus**: Implementation of the OSDI'16 paper "Consolidating Concurrency Control and Consensus for Commits under Conflicts"
- **Mako**: A speculative distributed transaction system with geo-replication (OSDI'25)

The codebase is primarily C++17 with multiple build systems (CMake, Makefile, WAF).

## Build Commands

### Important: Build Time Expectations
**WARNING**: This is a large C++ project with extensive template usage and multiple dependencies. Build times can be significant:
- **Initial full build**: 10-30 minutes depending on CPU and parallelism
- **Incremental builds**: 2-10 minutes depending on changes
- **Docker image build**: 10-30 minutes for first build
- **RustyCpp borrow checking**: Adds 1-2 minutes per file

**When running build commands, DO NOT use short timeouts (e.g., 30s, 60s, 120s). Use longer timeouts or no timeout:**
- For full builds: Use at least 30 minutes timeout (1800000ms)
- For incremental builds: Use at least 10 minutes timeout (600000ms)
- For Docker builds: Use at least 30 minutes timeout
- Better: Don't specify a timeout and let the build complete naturally

### Primary Build (CMake - Recommended for Mako)
```bash
# Configure and build
make clean
make -j32 
```

## Testing Commands

```bash
# run all experiments
./ci/ci.sh all

# simple transactions
./ci/ci.sh simpleTransaction

# simple replication
./ci/ci.sh simplePaxos

# two shards without replication
./ci/ci.sh shardNoReplication

# 1 shard with replication on dbtest
./ci/ci.sh shard1Replication

# 2 shards with replication on dbtest
./ci/ci.sh shard2Replication

# 1 shard with replication on simple transaction
./ci/ci.sh shard1ReplicationSimple

# 2 shards with replication on simple transaction
./ci/ci.sh shard2ReplicationSimple
```

## Code Architecture

### Core Directory Structure
- `src/deptran/`: Transaction protocol implementations (Janus, 2PL, OCC, RCC, Paxos, TAPIR, Snow)
- `src/mako/`: Mako system with Masstree storage engine and speculative execution
- `src/bench/`: Benchmark implementations (TPC-C, TPC-A, RW, Micro)
- `src/rrr/`: Custom RPC framework and networking layer
- `config/`: YAML configuration files for experiments and cluster topology

### Key Protocol Implementations
The system implements multiple distributed transaction protocols:
- **Janus** (`src/deptran/janus/`): Main protocol with graph-based dependency tracking
- **2PL** (`src/deptran/2pl/`): Traditional two-phase locking
- **OCC** (`src/deptran/occ/`): Optimistic concurrency control
- **RCC/Rococo** (`src/deptran/rcc/`): Distributed consensus protocol
- **Paxos** (`src/deptran/paxos/`): Consensus for replication

### Transport Layer Architecture
The system supports multiple transport mechanisms:
- Standard Ethernet via `src/rrr/` RPC framework
- DPDK for kernel bypass (`DPDK_ENABLED` flag)
- InfiniBand/RDMA support (`src/deptran/rcc_rpc.cpp`)
- eRPC integration for high-performance networking

### Configuration System
- **Host configuration**: `config/hosts*.yml` defines cluster topology and network settings
- **Benchmark configuration**: YAML files specify workload parameters
- **Build configuration**: Controlled via CMake flags or Makefile variables (SHARDS, PAXOS_LIB_ENABLED, etc.)

### Key Classes and Components
- `TxnCoordinator`: Coordinates distributed transactions across shards
- `TxnScheduler`: Handles transaction scheduling and execution
- `Communicator`: Manages RPC communication between nodes
- `Frame`: Protocol-specific transaction processing logic
- `Masstree`: High-performance in-memory index structure (Mako)

### Memory Management
- Uses jemalloc for optimized memory allocation
- Lock-free data structures in performance-critical paths
- Custom memory pools for reduced allocation overhead

## Development Notes

### Writing Safe C++ Code (Following RustyCpp Guidelines)
When writing new C++ code or modifying existing code, follow these safety guidelines to ensure compatibility with RustyCpp borrow checking:

#### Memory Safety Rules
1. **Ownership**: Every object should have a single owner at any given time
2. **Borrowing**: Use references (`&`) for read-only access, avoid raw pointers when possible
3. **Lifetime**: Ensure references don't outlive the objects they refer to
4. **Move Semantics**: Prefer `std::move` for transferring ownership, avoid use-after-move

#### Best Practices for RustyCpp Compliance
- **Smart Pointers**: Use `std::unique_ptr` for single ownership, `std::shared_ptr` for shared ownership
- **RAII**: Always use RAII (Resource Acquisition Is Initialization) patterns
- **Const Correctness**: Mark methods and parameters `const` when they don't modify state
- **Avoid Global State**: Minimize global variables and static mutable state
- **Reference Parameters**: Prefer `const&` for input parameters, avoid non-const references when possible
- **Return Values**: Return by value for small objects, use move semantics for large objects

#### Common Patterns to Avoid
- Double deletion or use-after-free
- Returning references to local variables
- Storing raw pointers without clear ownership
- Circular references without weak pointers
- Mutable aliasing (multiple mutable references to the same object)

#### Borrow Checking Integration
The project uses RustyCpp for static analysis. To ensure your code passes borrow checking:
- Build with `ENABLE_BORROW_CHECKING=ON` during development
- Run `make borrow_check_all_dbtest` to verify all files
- Address any violations before committing

### Adding New Transaction Protocols
New protocols should be added under `src/deptran/` following the existing pattern:
1. Create protocol directory with coordinator, scheduler, and frame implementations
2. Register in `src/deptran/frame.cc` and `src/deptran/scheduler.cc`
3. Add configuration support in benchmark YAML files

### Modifying Benchmarks
Benchmarks are in `src/bench/`. Each benchmark has:
- Workload generator (`*_workload.cc`)
- Piece registration (`*_pieces.cc`)
- Stored procedures (`*_procedures.cc`)

### Debugging
- Use `MODE=debug` for debug builds with symbols
- Enable logging with environment variables or config files
- Use `gdb` or `lldb` with the generated executables

### Performance Profiling
- Build with `MODE=perf` for optimized builds
- Use Google perftools (linked automatically)
- Profile with `perf record` and analyze with `perf report`