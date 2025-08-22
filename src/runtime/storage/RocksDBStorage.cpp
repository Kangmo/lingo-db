#ifdef WITH_ROCKSDB

#include "lingodb/runtime/storage/RocksDBStorage.h"
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/cache.h>
#include <filesystem>
#include <stdexcept>

namespace lingodb::runtime {

RocksDBStorage::RocksDBStorage(const std::string& path) : db_path(path) {}

RocksDBStorage::~RocksDBStorage() {
    if (opened) {
        close();
    }
}

rocksdb::Status RocksDBStorage::open() {
    if (opened) {
        return rocksdb::Status::InvalidArgument("Database already opened");
    }

    // Create directory if it doesn't exist
    try {
        if (!std::filesystem::exists(db_path)) {
            std::filesystem::create_directories(db_path);
        }
    } catch (const std::filesystem::filesystem_error& e) {
        return rocksdb::Status::IOError("Failed to create directory: " + std::string(e.what()));
    }

    // Check if this is a new database
    bool is_new_db = !databaseExists(db_path);
    
    rocksdb::DB* db_ptr;
    rocksdb::Status status;
    
    if (is_new_db) {
        // Create new database with column families using the proper creation method
        status = createDatabase(db_path);
        if (!status.ok()) {
            return status;
        }
        
        // Now open the newly created database with all column families
        rocksdb::Options options = getDefaultOptions();
        auto cf_descriptors = getColumnFamilyDescriptors();
        std::vector<rocksdb::ColumnFamilyHandle*> handles;
        
        status = rocksdb::DB::Open(options, db_path, cf_descriptors, &handles, &db_ptr);
        if (status.ok()) {
            column_families = std::move(handles);
        }
    } else {
        // Open existing database
        rocksdb::Options options = getDefaultOptions();
        auto cf_descriptors = getColumnFamilyDescriptors();
        std::vector<rocksdb::ColumnFamilyHandle*> handles;
        
        status = rocksdb::DB::Open(options, db_path, cf_descriptors, &handles, &db_ptr);
        if (status.ok()) {
            column_families = std::move(handles);
        }
    }
    
    if (status.ok()) {
        db.reset(db_ptr);
        opened = true;
    }
    
    return status;
}

rocksdb::Status RocksDBStorage::close() {
    if (!opened) {
        return rocksdb::Status::OK();
    }
    
    // Close column family handles
    for (auto* handle : column_families) {
        delete handle;
    }
    column_families.clear();
    
    // Close database
    rocksdb::Status status = db->Close();
    db.reset();
    opened = false;
    
    return status;
}

rocksdb::Status RocksDBStorage::put(ColumnFamily cf, const std::string& key, const std::string& value) {
    if (!opened) {
        return rocksdb::Status::InvalidArgument("Database not opened");
    }
    
    rocksdb::WriteOptions write_options;
    return db->Put(write_options, getColumnFamily(cf), key, value);
}

rocksdb::Status RocksDBStorage::get(ColumnFamily cf, const std::string& key, std::string* value) {
    if (!opened) {
        return rocksdb::Status::InvalidArgument("Database not opened");
    }
    
    rocksdb::ReadOptions read_options;
    return db->Get(read_options, getColumnFamily(cf), key, value);
}

rocksdb::Status RocksDBStorage::del(ColumnFamily cf, const std::string& key) {
    if (!opened) {
        return rocksdb::Status::InvalidArgument("Database not opened");
    }
    
    rocksdb::WriteOptions write_options;
    return db->Delete(write_options, getColumnFamily(cf), key);
}

rocksdb::Status RocksDBStorage::writeBatch(const std::vector<std::tuple<ColumnFamily, std::string, std::string>>& operations) {
    if (!opened) {
        return rocksdb::Status::InvalidArgument("Database not opened");
    }
    
    rocksdb::WriteBatch batch;
    for (const auto& [cf, key, value] : operations) {
        batch.Put(getColumnFamily(cf), key, value);
    }
    
    rocksdb::WriteOptions write_options;
    return db->Write(write_options, &batch);
}

std::unique_ptr<rocksdb::Iterator> RocksDBStorage::newIterator(ColumnFamily cf) {
    if (!opened) {
        return nullptr;
    }
    
    rocksdb::ReadOptions read_options;
    return std::unique_ptr<rocksdb::Iterator>(db->NewIterator(read_options, getColumnFamily(cf)));
}

bool RocksDBStorage::exists(ColumnFamily cf, const std::string& key) {
    std::string value;
    rocksdb::Status status = get(cf, key, &value);
    return status.ok();
}

rocksdb::Status RocksDBStorage::flush(ColumnFamily cf) {
    if (!opened) {
        return rocksdb::Status::InvalidArgument("Database not opened");
    }
    
    rocksdb::FlushOptions flush_options;
    return db->Flush(flush_options, getColumnFamily(cf));
}

rocksdb::ColumnFamilyHandle* RocksDBStorage::getColumnFamily(ColumnFamily cf) {
    size_t index = static_cast<size_t>(cf);
    if (index >= column_families.size()) {
        throw std::out_of_range("Invalid column family index");
    }
    return column_families[index];
}

rocksdb::Status RocksDBStorage::createDatabase(const std::string& path) {
    // Validate path
    if (path.empty()) {
        return rocksdb::Status::InvalidArgument("Database path cannot be empty");
    }
    
    try {
        if (!std::filesystem::exists(path)) {
            std::filesystem::create_directories(path);
        }
    } catch (const std::filesystem::filesystem_error& e) {
        return rocksdb::Status::IOError("Failed to create directory: " + std::string(e.what()));
    }
    
    // First create database with default column family only
    rocksdb::Options options;
    options.create_if_missing = true;
    
    rocksdb::DB* db;
    rocksdb::Status status = rocksdb::DB::Open(options, path, &db);
    
    if (status.ok()) {
        // Create additional column families
        rocksdb::ColumnFamilyOptions cf_options;
        rocksdb::ColumnFamilyHandle* cf_handle;
        
        status = db->CreateColumnFamily(cf_options, "catalog", &cf_handle);
        if (status.ok()) {
            delete cf_handle;
            status = db->CreateColumnFamily(cf_options, "tables", &cf_handle);
            if (status.ok()) {
                delete cf_handle;
                status = db->CreateColumnFamily(cf_options, "indexes", &cf_handle);
                if (status.ok()) {
                    delete cf_handle;
                    status = db->CreateColumnFamily(cf_options, "metadata", &cf_handle);
                    if (status.ok()) {
                        delete cf_handle;
                    }
                }
            }
        }
        delete db;
    }
    
    return status;
}

bool RocksDBStorage::databaseExists(const std::string& path) {
    return std::filesystem::exists(path) && 
           std::filesystem::exists(path + "/CURRENT");
}

rocksdb::Options RocksDBStorage::getDefaultOptions() {
    rocksdb::Options options;
    
    // Basic options
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    
    // Performance tuning
    options.write_buffer_size = 64 * 1024 * 1024; // 64MB
    options.max_write_buffer_number = 3;
    options.target_file_size_base = 64 * 1024 * 1024; // 64MB
    options.max_background_jobs = 4;
    
    // Compression
    options.compression = rocksdb::kSnappyCompression;
    
    // Block cache for better read performance
    rocksdb::BlockBasedTableOptions table_options;
    table_options.block_cache = rocksdb::NewLRUCache(256 * 1024 * 1024); // 256MB cache
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
    
    return options;
}

std::vector<rocksdb::ColumnFamilyDescriptor> RocksDBStorage::getColumnFamilyDescriptors() {
    rocksdb::ColumnFamilyOptions cf_options;
    
    std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
    descriptors.emplace_back(rocksdb::kDefaultColumnFamilyName, cf_options);
    descriptors.emplace_back("catalog", cf_options);
    descriptors.emplace_back("tables", cf_options);
    descriptors.emplace_back("indexes", cf_options);
    descriptors.emplace_back("metadata", cf_options);
    
    return descriptors;
}

// RocksDBIterator implementation
RocksDBIterator::RocksDBIterator(std::unique_ptr<rocksdb::Iterator> iter) 
    : iterator(std::move(iter)) {}

void RocksDBIterator::SeekToFirst() {
    iterator->SeekToFirst();
}

void RocksDBIterator::Seek(const std::string& key) {
    iterator->Seek(key);
}

void RocksDBIterator::Next() {
    iterator->Next();
}

bool RocksDBIterator::Valid() const {
    return iterator->Valid();
}

std::string RocksDBIterator::key() const {
    return iterator->key().ToString();
}

std::string RocksDBIterator::value() const {
    return iterator->value().ToString();
}

rocksdb::Status RocksDBIterator::status() const {
    return iterator->status();
}

} // namespace lingodb::runtime

#endif // WITH_ROCKSDB 