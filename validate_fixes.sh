#!/bin/bash

echo "🔍 Validating RocksDB Integration Fixes..."

# Function to check if a pattern exists in a file
check_pattern() {
    local file="$1"
    local pattern="$2"
    local description="$3"
    
    if [ ! -f "$file" ]; then
        echo "❌ File not found: $file"
        return 1
    fi
    
    if grep -q "$pattern" "$file"; then
        echo "✅ $description"
        return 0
    else
        echo "❌ Missing: $description in $file"
        return 1
    fi
}

# Function to check if a pattern does NOT exist in a file
check_not_pattern() {
    local file="$1"
    local pattern="$2"
    local description="$3"
    
    if [ ! -f "$file" ]; then
        echo "❌ File not found: $file"
        return 1
    fi
    
    if ! grep -q "$pattern" "$file"; then
        echo "✅ $description"
        return 0
    else
        echo "❌ Found problematic pattern: $description in $file"
        return 1
    fi
}

echo ""
echo "📋 Checking RocksDB Iterator API fixes..."

# Check that we use lowercase key() method, not Key()
check_pattern "src/runtime/storage/RocksDBTableStorage.cpp" "iterator->key().ToString()" "Using correct iterator->key() method"
check_pattern "src/catalog/RocksDBCatalog.cpp" "iterator->key().ToString()" "Using correct iterator->key() method"
check_not_pattern "src/runtime/storage/RocksDBTableStorage.cpp" "iterator->Key()" "No incorrect Key() calls"
check_not_pattern "src/catalog/RocksDBCatalog.cpp" "iterator->Key()" "No incorrect Key() calls"

echo ""
echo "📋 Checking required headers..."

# Check for required includes
check_pattern "src/runtime/storage/RocksDBStorage.cpp" "#include <rocksdb/cache.h>" "RocksDB cache header included"
check_pattern "src/runtime/storage/RocksDBTableStorage.cpp" "#include \"lingodb/catalog/Types.h\"" "Catalog Types header included"
check_pattern "src/runtime/storage/RocksDBTableStorage.cpp" "#include <cassert>" "Assert header included"
check_pattern "src/catalog/RocksDBCatalog.cpp" "#include <iostream>" "iostream header included"

echo ""
echo "📋 Checking column family configuration..."

# Check column family enum values
check_pattern "include/lingodb/runtime/storage/RocksDBStorage.h" "CATALOG = 1" "Correct column family indices"
check_pattern "include/lingodb/runtime/storage/RocksDBStorage.h" "METADATA = 4" "Correct metadata CF index"

echo ""
echo "📋 Checking conditional compilation..."

# Check for proper WITH_ROCKSDB guards
check_pattern "src/runtime/storage/RocksDBStorage.cpp" "#ifdef WITH_ROCKSDB" "RocksDB conditional compilation"
check_pattern "src/runtime/storage/RocksDBTableStorage.cpp" "#ifdef WITH_ROCKSDB" "RocksDBTableStorage conditional compilation"
check_pattern "src/catalog/RocksDBCatalog.cpp" "#ifdef WITH_ROCKSDB" "RocksDBCatalog conditional compilation"

echo ""
echo "📋 Checking CMake configuration..."

# Check CMake setup
check_pattern "CMakeLists.txt" "find_package(RocksDB QUIET)" "RocksDB package discovery"
check_pattern "CMakeLists.txt" "add_definitions(-DWITH_ROCKSDB)" "RocksDB compile definition"

echo ""
echo "🎯 Testing basic compilation..."

# Try to compile a simple test
cat > /tmp/rocksdb_test.cpp << 'EOF'
#ifdef WITH_ROCKSDB
#include "include/lingodb/runtime/storage/RocksDBStorage.h"
int main() { return 0; }
#else
int main() { return 0; }
#endif
EOF

if g++ -I. -DWITH_ROCKSDB -c /tmp/rocksdb_test.cpp -o /tmp/rocksdb_test.o 2>/dev/null; then
    echo "✅ Basic compilation test passed"
    rm -f /tmp/rocksdb_test.o
else
    echo "❌ Basic compilation test failed"
fi

rm -f /tmp/rocksdb_test.cpp

echo ""
echo "🏁 Validation complete!"
echo ""
echo "📝 Next steps:"
echo "   1. Run: make build-debug"
echo "   2. If successful, run: make run-test"
echo "   3. Check COMPILATION_FIXES.md for troubleshooting" 