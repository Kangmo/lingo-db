#!/bin/bash

echo "Testing RocksDB Integration Compilation..."

# Create a simple test directory
mkdir -p build_test
cd build_test

# Configure with CMake
echo "Configuring with CMake..."
cmake -G Ninja .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_C_COMPILER=clang

# Try to build just the runtime and catalog libraries
echo "Building runtime and catalog..."
ninja runtime catalog

if [ $? -eq 0 ]; then
    echo "✅ RocksDB integration compiled successfully!"
else
    echo "❌ Compilation failed - checking errors..."
    # Try to build with verbose output
    ninja runtime catalog -v
fi

cd ..
echo "Test completed." 