#!/bin/bash
# GFS Master Test Runner Script
# Runs all comprehensive tests and generates reports

echo "================================================================================"
echo "                    GFS COMPREHENSIVE TEST SUITE RUNNER"
echo "================================================================================"
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "[ERROR] Docker is not running. Please start Docker first."
    exit 1
fi

# Check if GFS containers are running
MASTER_COUNT=$(docker ps --filter "name=master" --format "{{.Names}}" | wc -l)
CHUNK_COUNT=$(docker ps --filter "name=chunkserver" --format "{{.Names}}" | wc -l)

echo "Current System Status:"
echo "  Masters running:      $MASTER_COUNT/3"
echo "  Chunk servers running: $CHUNK_COUNT/3"
echo ""

if [ "$MASTER_COUNT" -lt 3 ] || [ "$CHUNK_COUNT" -lt 3 ]; then
    echo "[WARNING] Not all containers are running!"
    echo "Would you like to start the system? (y/n)"
    read -r response
    if [[ "$response" == "y" ]]; then
        echo "Starting GFS system..."
        ./start_masters.sh 3 --clean
        ./start_chunkservers.sh 3 --clean
        echo "Waiting for system to stabilize..."
        sleep 5
    else
        echo "Please start the system manually:"
        echo "  ./start_masters.sh 3 --clean"
        echo "  ./start_chunkservers.sh 3 --clean"
        exit 1
    fi
fi

echo "================================================================================"
echo ""
echo "Select test suite to run:"
echo ""
echo "  1) Comprehensive Tests (28 tests - All functionalities)"
echo "  2) Fault Tolerance Tests (4 tests - Failover & Recovery)"
echo "  3) Both Suites (Complete testing)"
echo "  4) Quick Smoke Test (Basic functionality only)"
echo ""
echo "================================================================================"
echo -n "Enter choice [1-4]: "
read choice

case $choice in
    1)
        echo ""
        echo "Running Comprehensive Tests..."
        echo "================================================================================"
        python3 comprehensive_tests.py
        ;;
    2)
        echo ""
        echo "Running Fault Tolerance Tests..."
        echo "================================================================================"
        python3 failover_tests.py
        ;;
    3)
        echo ""
        echo "Running All Tests (This will take several minutes)..."
        echo "================================================================================"
        echo ""
        echo "[1/2] Running Comprehensive Tests..."
        python3 comprehensive_tests.py
        COMP_EXIT=$?
        
        echo ""
        echo "[2/2] Running Fault Tolerance Tests..."
        python3 failover_tests.py
        FAIL_EXIT=$?
        
        echo ""
        echo "================================================================================"
        echo "                        ALL TESTS COMPLETED"
        echo "================================================================================"
        
        if [ $COMP_EXIT -eq 0 ] && [ $FAIL_EXIT -eq 0 ]; then
            echo "[SUCCESS] All test suites passed!"
        else
            echo "[WARNING] Some tests failed. Check logs for details."
        fi
        ;;
    4)
        echo ""
        echo "Running Quick Smoke Test..."
        echo "================================================================================"
        python3 -c "
from client import MultiMasterGFSClient
import sys

MASTER_ADDRESSES = ['10.174.155.37:9010', '10.174.155.37:9011', '10.174.155.37:9012']

try:
    print('Connecting to GFS...')
    client = MultiMasterGFSClient(MASTER_ADDRESSES)
    print(f'[OK] Connected! Leader: {client.current_leader}')
    
    print('Testing file creation...')
    client.create_file('/smoke_test.txt')
    print('[OK] File created')
    
    print('Testing file write...')
    client.write_file('/smoke_test.txt', b'Smoke test data')
    print('[OK] Data written')
    
    print('Testing file read...')
    data = client.read_file('/smoke_test.txt')
    assert data == b'Smoke test data'
    print('[OK] Data read correctly')
    
    print('Testing cluster status...')
    status = client.get_cluster_status()
    print(f'[OK] Primary: {status[\"primary_master\"][\"master_id\"]}')
    
    print('')
    print('[SUCCESS] Smoke test passed! System is operational.')
    sys.exit(0)
except Exception as e:
    print(f'[FAILED] Smoke test failed: {e}')
    sys.exit(1)
"
        ;;
    *)
        echo "[ERROR] Invalid choice"
        exit 1
        ;;
esac

echo ""
echo "================================================================================"
echo "                           TEST EXECUTION COMPLETE"
echo "================================================================================"
echo ""
echo "Test logs are saved in: ./test_logs/"
echo ""
echo "To view logs:"
echo "  ls -lh test_logs/"
echo "  cat test_logs/<latest_log_file>"
echo ""
echo "To view JSON results:"
echo "  cat test_logs/<latest_json_file> | python3 -m json.tool"
echo ""
echo "================================================================================"
