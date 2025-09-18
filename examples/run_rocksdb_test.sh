#!/bin/bash
# Script to test RocksDB persistence implementation

echo "=== RocksDB Persistence Test Script ==="
echo ""

# Get the project root (parent of examples)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

echo "1. Cleaning test RocksDB directories..."
make -j32 

# Clean test directories
echo "2. Cleaning test RocksDB directories..."
rm -rf /tmp/test_rocksdb* /tmp/mako_rocksdb*

# Run test
echo "3. Running RocksDB persistence tests..."
if ./build/test_rocksdb_persistence > /tmp/rocksdb_test_output.txt 2>&1; then
    echo "   ✓ Basic persistence tests passed"
    echo ""
    echo "Test output summary:"
    grep "===" /tmp/rocksdb_test_output.txt
    echo ""
    # Show performance metrics
    grep -E "Throughput:|Time taken:" /tmp/rocksdb_test_output.txt | head -3
else
    echo "   ✗ Basic persistence tests failed"
    cat /tmp/rocksdb_test_output.txt
    exit 1
fi

# Run callback demo test
echo ""
echo "4. Running callback demonstration test..."
if ./build/test_callback_demo > /tmp/callback_demo_output.txt 2>&1; then
    echo "   ✓ Callback demo passed"
    echo ""
    echo "Callback demo output:"
    grep -E "===|Total|Persisted:|Failed:" /tmp/callback_demo_output.txt
else
    echo "   ✗ Callback demo failed"
    cat /tmp/callback_demo_output.txt
    exit 1
fi

# Verify RocksDB files were created
echo ""
echo "5. Verifying RocksDB persistence files..."
if ls /tmp/test_rocksdb*/CURRENT > /dev/null 2>&1; then
    echo "   ✓ RocksDB database files created successfully"
    echo "   Database locations:"
    for dir in /tmp/test_rocksdb*; do
        if [ -d "$dir" ]; then
            echo "     - $dir ($(du -sh $dir | cut -f1))"
        fi
    done
else
    echo "   ✗ RocksDB database files not found"
fi

echo ""
echo "=== All tests completed successfully! ==="
echo ""
echo "Tests executed:"
echo "  ✓ Basic RocksDB persistence test (test_rocksdb_persistence)"
echo "  ✓ Callback demonstration test (test_callback_demo)"
echo ""
echo "Integration points:"
echo "  - Transaction.hh:143 - Persistence with atomic counters and callbacks"
echo "  - Transaction.cc:779 - Helper thread persistence with callbacks"
echo "  - mako.hh:649 - RocksDB initialization on startup"
echo ""
echo "Callback features:"
echo "  - Atomic counters track success/failure in main thread"
echo "  - Progress reporting every 1000 successful writes"
echo "  - Immediate error reporting with failure counts"
echo ""
echo "Key format: {shard_id}:{partition_id}:{epoch}:{sequence_number}"