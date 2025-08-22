#include "catch2/catch_test_macros.hpp"

#ifdef WITH_ROCKSDB

#include "lingodb/runtime/storage/RocksDBStorage.h"
#include <filesystem>
#include <fstream>

using namespace lingodb::runtime;
namespace fs = std::filesystem;

namespace {
    void cleanupTestDirectory(const std::string& path) {
        if (fs::exists(path)) {
            fs::remove_all(path);
        }
    }
}

TEST_CASE("RocksDBStorage: Basic Operations", "[rocksdbs][storage][basic]") {
    const std::string testDbPath = "/tmp/lingodb_test_rocksdb_storage_basic";
    cleanupTestDirectory(testDbPath);
    
    SECTION("Database creation and opening") {
        // Test database creation
        auto status = RocksDBStorage::createDatabase(testDbPath);
        REQUIRE(status.ok());
        REQUIRE(fs::exists(testDbPath));
        
        // Test opening existing database
        auto storage = std::make_shared<RocksDBStorage>(testDbPath);
        status = storage->open();
        REQUIRE(status.ok());
        
        // Test database existence check
        REQUIRE(RocksDBStorage::databaseExists(testDbPath));
        
        status = storage->close();
        REQUIRE(status.ok());
    }
    
    SECTION("Database path validation") {
        // Test with non-existent path
        REQUIRE_FALSE(RocksDBStorage::databaseExists("/non/existent/path"));
        
        // Test opening non-existent database
        auto storage = std::make_shared<RocksDBStorage>("/non/existent/path");
        auto status = storage->open();
        REQUIRE_FALSE(status.ok());
    }
    
    cleanupTestDirectory(testDbPath);
}

TEST_CASE("RocksDBStorage: Put/Get/Delete Operations", "[rocksdbs][storage][crud]") {
    const std::string testDbPath = "/tmp/lingodb_test_rocksdb_storage_crud";
    cleanupTestDirectory(testDbPath);
    
    auto status = RocksDBStorage::createDatabase(testDbPath);
    REQUIRE(status.ok());
    
    auto storage = std::make_shared<RocksDBStorage>(testDbPath);
    status = storage->open();
    REQUIRE(status.ok());
    
    SECTION("Basic put and get operations") {
        // Test put operation
        status = storage->put(RocksDBStorage::ColumnFamily::CATALOG, "test_key", "test_value");
        REQUIRE(status.ok());
        
        // Test get operation
        std::string value;
        status = storage->get(RocksDBStorage::ColumnFamily::CATALOG, "test_key", &value);
        REQUIRE(status.ok());
        REQUIRE(value == "test_value");
        
        // Test exists operation
        REQUIRE(storage->exists(RocksDBStorage::ColumnFamily::CATALOG, "test_key"));
        REQUIRE_FALSE(storage->exists(RocksDBStorage::ColumnFamily::CATALOG, "non_existent_key"));
    }
    
    SECTION("Get non-existent key") {
        std::string value;
        status = storage->get(RocksDBStorage::ColumnFamily::CATALOG, "non_existent", &value);
        REQUIRE_FALSE(status.ok());
        REQUIRE(status.IsNotFound());
    }
    
    SECTION("Delete operations") {
        // Put a key first
        status = storage->put(RocksDBStorage::ColumnFamily::CATALOG, "delete_test", "value");
        REQUIRE(status.ok());
        REQUIRE(storage->exists(RocksDBStorage::ColumnFamily::CATALOG, "delete_test"));
        
        // Delete the key
        status = storage->del(RocksDBStorage::ColumnFamily::CATALOG, "delete_test");
        REQUIRE(status.ok());
        REQUIRE_FALSE(storage->exists(RocksDBStorage::ColumnFamily::CATALOG, "delete_test"));
        
        // Try to delete non-existent key (should still succeed)
        status = storage->del(RocksDBStorage::ColumnFamily::CATALOG, "non_existent");
        REQUIRE(status.ok());
    }
    
    SECTION("Multiple column families") {
        // Test operations on different column families
        status = storage->put(RocksDBStorage::ColumnFamily::CATALOG, "key1", "catalog_value");
        REQUIRE(status.ok());
        
        status = storage->put(RocksDBStorage::ColumnFamily::TABLES, "key1", "tables_value");
        REQUIRE(status.ok());
        
        // Verify they're stored separately
        std::string catalogValue, tablesValue;
        status = storage->get(RocksDBStorage::ColumnFamily::CATALOG, "key1", &catalogValue);
        REQUIRE(status.ok());
        REQUIRE(catalogValue == "catalog_value");
        
        status = storage->get(RocksDBStorage::ColumnFamily::TABLES, "key1", &tablesValue);
        REQUIRE(status.ok());
        REQUIRE(tablesValue == "tables_value");
    }
    
    storage->close();
    cleanupTestDirectory(testDbPath);
}

TEST_CASE("RocksDBStorage: Batch Operations", "[rocksdbs][storage][batch]") {
    const std::string testDbPath = "/tmp/lingodb_test_rocksdb_storage_batch";
    cleanupTestDirectory(testDbPath);
    
    auto status = RocksDBStorage::createDatabase(testDbPath);
    REQUIRE(status.ok());
    
    auto storage = std::make_shared<RocksDBStorage>(testDbPath);
    status = storage->open();
    REQUIRE(status.ok());
    
    SECTION("Batch write operations") {
        std::vector<std::tuple<RocksDBStorage::ColumnFamily, std::string, std::string>> operations = {
            {RocksDBStorage::ColumnFamily::CATALOG, "batch_key1", "batch_value1"},
            {RocksDBStorage::ColumnFamily::CATALOG, "batch_key2", "batch_value2"},
            {RocksDBStorage::ColumnFamily::TABLES, "batch_key3", "batch_value3"}
        };
        
        status = storage->writeBatch(operations);
        REQUIRE(status.ok());
        
        // Verify all keys were written
        std::string value;
        status = storage->get(RocksDBStorage::ColumnFamily::CATALOG, "batch_key1", &value);
        REQUIRE(status.ok());
        REQUIRE(value == "batch_value1");
        
        status = storage->get(RocksDBStorage::ColumnFamily::CATALOG, "batch_key2", &value);
        REQUIRE(status.ok());
        REQUIRE(value == "batch_value2");
        
        status = storage->get(RocksDBStorage::ColumnFamily::TABLES, "batch_key3", &value);
        REQUIRE(status.ok());
        REQUIRE(value == "batch_value3");
    }
    
    SECTION("Empty batch operations") {
        std::vector<std::tuple<RocksDBStorage::ColumnFamily, std::string, std::string>> emptyOps;
        status = storage->writeBatch(emptyOps);
        REQUIRE(status.ok());
    }
    
    storage->close();
    cleanupTestDirectory(testDbPath);
}

TEST_CASE("RocksDBStorage: Iterator Operations", "[rocksdbs][storage][iterator]") {
    const std::string testDbPath = "/tmp/lingodb_test_rocksdb_storage_iterator";
    cleanupTestDirectory(testDbPath);
    
    auto status = RocksDBStorage::createDatabase(testDbPath);
    REQUIRE(status.ok());
    
    auto storage = std::make_shared<RocksDBStorage>(testDbPath);
    status = storage->open();
    REQUIRE(status.ok());
    
    SECTION("Iterator over multiple keys") {
        // Insert test data
        std::map<std::string, std::string> testData = {
            {"key_a", "value_a"},
            {"key_b", "value_b"},
            {"key_c", "value_c"},
            {"key_d", "value_d"}
        };
        
        for (const auto& [key, value] : testData) {
            status = storage->put(RocksDBStorage::ColumnFamily::CATALOG, key, value);
            REQUIRE(status.ok());
        }
        
        // Test iterator
        auto iterator = storage->newIterator(RocksDBStorage::ColumnFamily::CATALOG);
        REQUIRE(iterator != nullptr);
        
        std::map<std::string, std::string> retrievedData;
        for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
            REQUIRE(iterator->status().ok());
            retrievedData[iterator->key().ToString()] = iterator->value().ToString();
        }
        
        REQUIRE(retrievedData == testData);
    }
    
    SECTION("Iterator on empty database") {
        auto iterator = storage->newIterator(RocksDBStorage::ColumnFamily::TABLES);
        REQUIRE(iterator != nullptr);
        
        iterator->SeekToFirst();
        REQUIRE_FALSE(iterator->Valid());
        REQUIRE(iterator->status().ok());
    }
    
    SECTION("Iterator seek operations") {
        // Insert test data
        status = storage->put(RocksDBStorage::ColumnFamily::CATALOG, "a_key", "a_value");
        REQUIRE(status.ok());
        status = storage->put(RocksDBStorage::ColumnFamily::CATALOG, "c_key", "c_value");
        REQUIRE(status.ok());
        status = storage->put(RocksDBStorage::ColumnFamily::CATALOG, "e_key", "e_value");
        REQUIRE(status.ok());
        
        auto iterator = storage->newIterator(RocksDBStorage::ColumnFamily::CATALOG);
        
        // Seek to existing key
        iterator->Seek("c_key");
        REQUIRE(iterator->Valid());
        REQUIRE(iterator->key().ToString() == "c_key");
        REQUIRE(iterator->value().ToString() == "c_value");
        
        // Seek to non-existent key (should find next greater key)
        iterator->Seek("b_key");
        REQUIRE(iterator->Valid());
        REQUIRE(iterator->key().ToString() == "c_key");
        
        // Seek beyond last key
        iterator->Seek("z_key");
        REQUIRE_FALSE(iterator->Valid());
    }
    
    storage->close();
    cleanupTestDirectory(testDbPath);
}

TEST_CASE("RocksDBStorage: Flush Operations", "[rocksdbs][storage][flush]") {
    const std::string testDbPath = "/tmp/lingodb_test_rocksdb_storage_flush";
    cleanupTestDirectory(testDbPath);
    
    auto status = RocksDBStorage::createDatabase(testDbPath);
    REQUIRE(status.ok());
    
    auto storage = std::make_shared<RocksDBStorage>(testDbPath);
    status = storage->open();
    REQUIRE(status.ok());
    
    SECTION("Flush operations") {
        // Add some data
        status = storage->put(RocksDBStorage::ColumnFamily::CATALOG, "flush_test", "flush_value");
        REQUIRE(status.ok());
        
        // Flush catalog column family
        status = storage->flush(RocksDBStorage::ColumnFamily::CATALOG);
        REQUIRE(status.ok());
        
        // Flush tables column family (should work even if empty)
        status = storage->flush(RocksDBStorage::ColumnFamily::TABLES);
        REQUIRE(status.ok());
        
        // Verify data is still accessible after flush
        std::string value;
        status = storage->get(RocksDBStorage::ColumnFamily::CATALOG, "flush_test", &value);
        REQUIRE(status.ok());
        REQUIRE(value == "flush_value");
    }
    
    storage->close();
    cleanupTestDirectory(testDbPath);
}

TEST_CASE("RocksDBStorage: Error Handling", "[rocksdbs][storage][errors]") {
    const std::string testDbPath = "/tmp/lingodb_test_rocksdb_storage_errors";
    cleanupTestDirectory(testDbPath);
    
    SECTION("Operations on unopened database") {
        auto storage = std::make_shared<RocksDBStorage>(testDbPath);
        // Don't call open()
        
        auto status = storage->put(RocksDBStorage::ColumnFamily::CATALOG, "key", "value");
        REQUIRE_FALSE(status.ok());
        REQUIRE(status.IsInvalidArgument());
        
        std::string value;
        status = storage->get(RocksDBStorage::ColumnFamily::CATALOG, "key", &value);
        REQUIRE_FALSE(status.ok());
        REQUIRE(status.IsInvalidArgument());
        
        status = storage->del(RocksDBStorage::ColumnFamily::CATALOG, "key");
        REQUIRE_FALSE(status.ok());
        REQUIRE(status.IsInvalidArgument());
        
        std::vector<std::tuple<RocksDBStorage::ColumnFamily, std::string, std::string>> ops;
        status = storage->writeBatch(ops);
        REQUIRE_FALSE(status.ok());
        REQUIRE(status.IsInvalidArgument());
        
        auto iterator = storage->newIterator(RocksDBStorage::ColumnFamily::CATALOG);
        REQUIRE(iterator == nullptr);
        
        status = storage->flush(RocksDBStorage::ColumnFamily::CATALOG);
        REQUIRE_FALSE(status.ok());
        REQUIRE(status.IsInvalidArgument());
    }
    
    SECTION("Double open") {
        auto status = RocksDBStorage::createDatabase(testDbPath);
        REQUIRE(status.ok());
        
        auto storage = std::make_shared<RocksDBStorage>(testDbPath);
        status = storage->open();
        REQUIRE(status.ok());
        
        // Try to open again
        status = storage->open();
        REQUIRE_FALSE(status.ok());
        REQUIRE(status.IsInvalidArgument());
        
        storage->close();
    }
    
    SECTION("Close unopened database") {
        auto storage = std::make_shared<RocksDBStorage>(testDbPath);
        auto status = storage->close();
        REQUIRE(status.ok()); // Should be OK to close unopened DB
    }
    
    cleanupTestDirectory(testDbPath);
}

TEST_CASE("RocksDBStorage: Large Data Operations", "[rocksdbs][storage][large]") {
    const std::string testDbPath = "/tmp/lingodb_test_rocksdb_storage_large";
    cleanupTestDirectory(testDbPath);
    
    auto status = RocksDBStorage::createDatabase(testDbPath);
    REQUIRE(status.ok());
    
    auto storage = std::make_shared<RocksDBStorage>(testDbPath);
    status = storage->open();
    REQUIRE(status.ok());
    
    SECTION("Large value storage and retrieval") {
        // Create a large value (1MB)
        std::string largeValue(1024 * 1024, 'X');
        
        status = storage->put(RocksDBStorage::ColumnFamily::TABLES, "large_key", largeValue);
        REQUIRE(status.ok());
        
        std::string retrievedValue;
        status = storage->get(RocksDBStorage::ColumnFamily::TABLES, "large_key", &retrievedValue);
        REQUIRE(status.ok());
        REQUIRE(retrievedValue == largeValue);
    }
    
    SECTION("Many small operations") {
        const int numOperations = 1000;
        
        // Insert many small key-value pairs
        for (int i = 0; i < numOperations; ++i) {
            std::string key = "key_" + std::to_string(i);
            std::string value = "value_" + std::to_string(i);
            
            status = storage->put(RocksDBStorage::ColumnFamily::CATALOG, key, value);
            REQUIRE(status.ok());
        }
        
        // Verify all were stored correctly
        for (int i = 0; i < numOperations; ++i) {
            std::string key = "key_" + std::to_string(i);
            std::string expectedValue = "value_" + std::to_string(i);
            std::string actualValue;
            
            status = storage->get(RocksDBStorage::ColumnFamily::CATALOG, key, &actualValue);
            REQUIRE(status.ok());
            REQUIRE(actualValue == expectedValue);
        }
        
        // Test iteration over all keys
        auto iterator = storage->newIterator(RocksDBStorage::ColumnFamily::CATALOG);
        int count = 0;
        for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
            REQUIRE(iterator->status().ok());
            count++;
        }
        REQUIRE(count == numOperations);
    }
    
    storage->close();
    cleanupTestDirectory(testDbPath);
}

#endif // WITH_ROCKSDB 