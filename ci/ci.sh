
#!/bin/bash

set -e  # Exit on error

# Function 1: Compile
compile() {
    make -j32
}

# Function 2: Run simple transaction test
run_simple_transaction() {
    ./build/simpleTransaction 
}

# Function 3: Run simple Paxos test
run_simple_paxos() {
    bash ./src/mako/update_config.sh 
    ./examples/simplePaxos.sh 
}

# Function 4: Run 2-shard no replication test
run_2shard_no_replication() {
    ./examples/test_2shard_no_replication.sh
}

# Main entry point with command parsing
case "${1:-}" in
    compile)
        compile
        ;;
    simpleTransaction)
        run_simple_transaction
        ;;
    simplePaxos)
        run_simple_paxos
        ;;
    2shardNoReplication)
        run_2shard_no_replication
        ;;
    all)
        # Run all steps in sequence
        compile
        run_simple_transaction
        run_simple_paxos
        run_2shard_no_replication
        echo "All CI steps completed successfully!"
        ;;
esac
