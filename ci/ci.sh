
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
    ./examples/simplePaxos.sh 
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
    all)
        # Run all steps in sequence
        compile
        run_simple_transaction
        run_simple_paxos
        log "All CI steps completed successfully!"
        ;;
esac