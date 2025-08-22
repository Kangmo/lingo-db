# LingoDB RocksDB Integration - Build and Test Guide

## Overview

This guide provides step-by-step instructions for building and testing the RocksDB integration in LingoDB.

## Prerequisites

### Required Dependencies
- **C++20 compatible compiler** (GCC 10+, Clang 12+)
- **CMake 3.13.4+**
- **Arrow 20.0**
- **LLVM/MLIR 20.1**
- **Ninja** (recommended build system)

### Optional Dependencies
- **RocksDB** (for persistent storage - automatically detected)
- **libsnappy** (compression, usually comes with RocksDB)

## Installation Guide

### 1. Install RocksDB (Optional but Recommended)

#### On macOS (using Homebrew):
```bash
brew install rocksdb
```

#### On Ubuntu/Debian:
```bash
sudo apt-get update
sudo apt-get install librocksdb-dev
```

#### From Source:
```bash
git clone https://github.com/facebook/rocksdb.git
cd rocksdb
make shared_lib
sudo make install-shared
```

### 2. Build LingoDB with RocksDB

```bash
# Navigate to LingoDB root directory
cd /path/to/lingo-db

# Create build directory
mkdir -p build/lingodb-debug

# Configure with CMake (RocksDB will be auto-detected)
cmake -G Ninja . -B build/lingodb-debug -DCMAKE_BUILD_TYPE=Debug

# Build the project
cmake --build build/lingodb-debug -- -j$(nproc)
```

### 3. Verify RocksDB Integration

Check the CMake output for RocksDB detection:
```bash
# You should see one of:
# "RocksDB found - enabling RocksDB storage backend"
# OR
# "RocksDB not found - RocksDB storage backend will be disabled"
```

## Testing the Integration

### 1. Run Full Test Suite

```bash
# Run all tests (includes RocksDB tests if enabled)
make run-test

# Or manually:
cd build/lingodb-debug
./tester                    # Unit tests
find ../../test/sqlite-small/ -name '*.test' | xargs -L 1 ./sqlite-tester  # SQL tests
```

### 2. Test RocksDB-Specific Features

#### Basic Table Operations:
```sql
-- Start LingoDB and run these commands:
set persist=1;

-- Create a table (will use RocksDB if available)
CREATE TABLE test_rocksdb (
    id BIGINT,
    name VARCHAR(50),
    value DOUBLE,
    PRIMARY KEY (id)
);

-- Insert data
INSERT INTO test_rocksdb VALUES 
    (1, 'test1', 1.5), 
    (2, 'test2', 2.5),
    (3, 'test3', 3.5);

-- Query data
SELECT * FROM test_rocksdb WHERE id = 1;
SELECT COUNT(*) FROM test_rocksdb;
```

#### Persistence Test:
```sql
-- 1. Create tables and insert data (as above)
-- 2. Exit LingoDB
-- 3. Restart LingoDB 
-- 4. Verify data is still there:
SELECT * FROM test_rocksdb;
```

### 3. Performance Comparison

Compare performance with/without RocksDB:

```bash
# With RocksDB (if available)
time ./build/lingodb-debug/sql resources/data/test/ < test_queries.sql

# Check storage size
du -sh resources/data/test/
```

## Build Configurations

### 1. Debug Build (Recommended for Development)
```bash
cmake -G Ninja . -B build/lingodb-debug -DCMAKE_BUILD_TYPE=Debug
cmake --build build/lingodb-debug
```

### 2. Release Build (Performance Testing)
```bash
cmake -G Ninja . -B build/lingodb-release -DCMAKE_BUILD_TYPE=Release
cmake --build build/lingodb-release
```

### 3. Without RocksDB (Fallback Mode)
```bash
# If RocksDB is not available or you want to disable it:
# The system will automatically fall back to the original implementation
# No special configuration needed
```

## Troubleshooting

### 1. RocksDB Not Found
**Problem**: CMake shows "RocksDB not found"
**Solution**: 
- Install RocksDB using package manager or build from source
- Ensure RocksDB headers and libraries are in standard paths
- Set `CMAKE_PREFIX_PATH` if RocksDB is in custom location

### 2. Compilation Errors
**Problem**: Compilation fails with RocksDB-related errors
**Solution**:
- Verify RocksDB version compatibility (6.0+)
- Check that all RocksDB dependencies are installed
- Try building without RocksDB first to verify base system

### 3. Runtime Errors
**Problem**: Database fails to start with RocksDB
**Solution**:
- Check disk space and permissions
- Verify database directory is writable
- Check RocksDB error messages in logs

### 4. Performance Issues
**Problem**: Slower performance with RocksDB
**Solution**:
- Verify disk I/O is not bottleneck
- Check RocksDB configuration in `RocksDBStorage.cpp`
- Try different compression settings

## Expected Behavior

### With RocksDB Available:
✅ Persistent storage across restarts  
✅ Improved scalability for large datasets  
✅ Better catalog performance with many tables  
✅ Crash recovery capabilities  

### Without RocksDB:
✅ All existing functionality preserved  
✅ In-memory table storage  
✅ File-based catalog persistence  
✅ No behavior changes from original system  

## Verification Steps

1. **Build Success**: Project compiles without errors
2. **Test Pass**: All existing tests continue to pass
3. **Persistence**: Data survives database restarts (with RocksDB)
4. **Performance**: Comparable or better performance
5. **Compatibility**: System works both with and without RocksDB

## File Locations

### Database Files (with RocksDB):
- `{database_dir}/` - RocksDB database directory
- `{database_dir}/CURRENT` - RocksDB current file
- `{database_dir}/*.sst` - RocksDB table files
- `{database_dir}/MANIFEST-*` - RocksDB manifest files

### Database Files (without RocksDB):
- `{database_dir}/db.lingodb` - Catalog file
- `{database_dir}/*.arrow` - Table data files

## Support

If you encounter issues:

1. Check this guide's troubleshooting section
2. Verify your environment matches the prerequisites  
3. Try building without RocksDB first to isolate issues
4. Check the implementation in `ROCKSDB_INTEGRATION_SUMMARY.md`

## Performance Tuning

For production use, consider:

1. **RocksDB Configuration**: Adjust settings in `RocksDBStorage::getDefaultOptions()`
2. **Cache Size**: Tune block cache size based on available memory
3. **Compression**: Experiment with different compression algorithms
4. **Storage**: Use fast SSDs for best performance

The RocksDB integration is designed to provide immediate benefits while being completely backward compatible with existing deployments. 