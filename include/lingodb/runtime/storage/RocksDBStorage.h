#ifndef LINGODB_RUNTIME_STORAGE_ROCKSDBSTORAGE_H
#define LINGODB_RUNTIME_STORAGE_ROCKSDBSTORAGE_H

#ifdef WITH_ROCKSDB
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/status.h>
#include <rocksdb/write_batch.h>
#include <memory>
#include <string>
#include <vector>

namespace lingodb::runtime {

/**
 * Wrapper class for RocksDB to provide a clean interface for storage operations
 */
class RocksDBStorage {
public:
    enum class ColumnFamily {
        CATALOG = 1,    // For catalog entries (tables, indexes, etc.)
        TABLES = 2,     // For user table data
        INDEXES = 3,    // For index data
        METADATA = 4    // For table metadata and statistics (0 is default CF)
    };

private:
    std::unique_ptr<rocksdb::DB> db;
    std::vector<rocksdb::ColumnFamilyHandle*> column_families;
    std::string db_path;
    bool opened = false;

public:
    explicit RocksDBStorage(const std::string& path);
    ~RocksDBStorage();

    // Database lifecycle
    rocksdb::Status open();
    rocksdb::Status close();
    
    // Basic operations
    rocksdb::Status put(ColumnFamily cf, const std::string& key, const std::string& value);
    rocksdb::Status get(ColumnFamily cf, const std::string& key, std::string* value);
    rocksdb::Status del(ColumnFamily cf, const std::string& key);
    
    // Batch operations
    rocksdb::Status writeBatch(const std::vector<std::tuple<ColumnFamily, std::string, std::string>>& operations);
    
    // Iterator operations
    std::unique_ptr<rocksdb::Iterator> newIterator(ColumnFamily cf);
    
    // Utility methods
    bool exists(ColumnFamily cf, const std::string& key);
    rocksdb::Status flush(ColumnFamily cf);
    
    // Get column family handle
    rocksdb::ColumnFamilyHandle* getColumnFamily(ColumnFamily cf);
    
    // Static methods for database management
    static rocksdb::Status createDatabase(const std::string& path);
    static bool databaseExists(const std::string& path);

private:
    void initializeColumnFamilies();
    rocksdb::Options getDefaultOptions();
    std::vector<rocksdb::ColumnFamilyDescriptor> getColumnFamilyDescriptors();
};

/**
 * RAII wrapper for RocksDB iterators
 */
class RocksDBIterator {
private:
    std::unique_ptr<rocksdb::Iterator> iterator;

public:
    explicit RocksDBIterator(std::unique_ptr<rocksdb::Iterator> iter);
    ~RocksDBIterator() = default;

    void SeekToFirst();
    void Seek(const std::string& key);
    void Next();
    bool Valid() const;
    std::string key() const;
    std::string value() const;
    rocksdb::Status status() const;
};

} // namespace lingodb::runtime

#endif // WITH_ROCKSDB

#endif // LINGODB_RUNTIME_STORAGE_ROCKSDBSTORAGE_H 