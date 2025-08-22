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

TEST_CASE("RocksDBStorage: Comprehensive Method Coverage", "[rocksdbs][comprehensive]") {
    const std::string testDbPath = "/tmp/lingodb_test_rocksdb_comprehensive";
    cleanupTestDirectory(testDbPath);
    
    auto status = RocksDBStorage::createDatabase(testDbPath);
    REQUIRE(status.ok());
    
    auto storage = std::make_shared<RocksDBStorage>(testDbPath);
    status = storage->open();
    REQUIRE(status.ok());

    SECTION("Batch operations") {
        // Test writeBatch with multiple column families
        std::vector<std::tuple<RocksDBStorage::ColumnFamily, std::string, std::string>> operations = {
            {RocksDBStorage::ColumnFamily::CATALOG, "batch_key1", "batch_value1"},
            {RocksDBStorage::ColumnFamily::TABLES, "batch_key2", "batch_value2"},
            {RocksDBStorage::ColumnFamily::INDEXES, "batch_key3", "batch_value3"},
            {RocksDBStorage::ColumnFamily::METADATA, "batch_key4", "batch_value4"}
        };
        
        status = storage->writeBatch(operations);
        REQUIRE(status.ok());
        
        // Verify all operations were applied
        for (const auto& [cf, key, expectedValue] : operations) {
            std::string retrievedValue;
            status = storage->get(cf, key, &retrievedValue);
            REQUIRE(status.ok());
            REQUIRE(retrievedValue == expectedValue);
        }
    }

    SECTION("Exists method") {
        // Test exists with key that exists
        status = storage->put(RocksDBStorage::ColumnFamily::CATALOG, "exists_test_key", "exists_test_value");
        REQUIRE(status.ok());
        
        REQUIRE(storage->exists(RocksDBStorage::ColumnFamily::CATALOG, "exists_test_key"));
        REQUIRE_FALSE(storage->exists(RocksDBStorage::ColumnFamily::CATALOG, "non_existent_key"));
        
        // Test exists across different column families
        REQUIRE_FALSE(storage->exists(RocksDBStorage::ColumnFamily::TABLES, "exists_test_key"));
    }

    SECTION("Flush operations") {
        // Add some data first
        status = storage->put(RocksDBStorage::ColumnFamily::CATALOG, "flush_test_key", "flush_test_value");
        REQUIRE(status.ok());
        
        // Test flush for each column family
        status = storage->flush(RocksDBStorage::ColumnFamily::CATALOG);
        REQUIRE(status.ok());
        
        status = storage->flush(RocksDBStorage::ColumnFamily::TABLES);
        REQUIRE(status.ok());
        
        status = storage->flush(RocksDBStorage::ColumnFamily::INDEXES);
        REQUIRE(status.ok());
        
        status = storage->flush(RocksDBStorage::ColumnFamily::METADATA);
        REQUIRE(status.ok());
        
        // Verify data is still accessible after flush
        std::string retrievedValue;
        status = storage->get(RocksDBStorage::ColumnFamily::CATALOG, "flush_test_key", &retrievedValue);
        REQUIRE(status.ok());
        REQUIRE(retrievedValue == "flush_test_value");
    }

    SECTION("Column family operations") {
        // Test operations across all column families
        const std::vector<RocksDBStorage::ColumnFamily> columnFamilies = {
            RocksDBStorage::ColumnFamily::CATALOG,
            RocksDBStorage::ColumnFamily::TABLES,
            RocksDBStorage::ColumnFamily::INDEXES,
            RocksDBStorage::ColumnFamily::METADATA
        };
        
        for (size_t i = 0; i < columnFamilies.size(); ++i) {
            auto cf = columnFamilies[i];
            std::string key = "cf_test_key_" + std::to_string(i);
            std::string value = "cf_test_value_" + std::to_string(i);
            
            // Test put/get for each column family
            status = storage->put(cf, key, value);
            REQUIRE(status.ok());
            
            std::string retrievedValue;
            status = storage->get(cf, key, &retrievedValue);
            REQUIRE(status.ok());
            REQUIRE(retrievedValue == value);
            
            // Test iterator for each column family
            auto iterator = storage->newIterator(cf);
            REQUIRE(iterator != nullptr);
            
            bool foundKey = false;
            for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
                if (iterator->key().ToString() == key) {
                    REQUIRE(iterator->value().ToString() == value);
                    foundKey = true;
                    break;
                }
            }
            REQUIRE(foundKey);
            
            // Test delete for each column family
            status = storage->del(cf, key);
            REQUIRE(status.ok());
            
            status = storage->get(cf, key, &retrievedValue);
            REQUIRE(status.IsNotFound());
        }
    }

    SECTION("getColumnFamily method") {
        // Test that getColumnFamily returns valid handles
        auto catalogHandle = storage->getColumnFamily(RocksDBStorage::ColumnFamily::CATALOG);
        REQUIRE(catalogHandle != nullptr);
        
        auto tablesHandle = storage->getColumnFamily(RocksDBStorage::ColumnFamily::TABLES);
        REQUIRE(tablesHandle != nullptr);
        
        auto indexesHandle = storage->getColumnFamily(RocksDBStorage::ColumnFamily::INDEXES);
        REQUIRE(indexesHandle != nullptr);
        
        auto metadataHandle = storage->getColumnFamily(RocksDBStorage::ColumnFamily::METADATA);
        REQUIRE(metadataHandle != nullptr);
        
        // Verify they are different handles
        REQUIRE(catalogHandle != tablesHandle);
        REQUIRE(catalogHandle != indexesHandle);
        REQUIRE(catalogHandle != metadataHandle);
    }

    SECTION("Error handling") {
        // Test operations on closed database
        storage->close();
        
        std::string value;
        status = storage->get(RocksDBStorage::ColumnFamily::CATALOG, "test_key", &value);
        REQUIRE_FALSE(status.ok());
        
        status = storage->put(RocksDBStorage::ColumnFamily::CATALOG, "test_key", "test_value");
        REQUIRE_FALSE(status.ok());
        
        // Reopen for cleanup
        status = storage->open();
        REQUIRE(status.ok());
    }

    storage->close();
    cleanupTestDirectory(testDbPath);
}

TEST_CASE("RocksDBStorage: Static Methods", "[rocksdbs][static]") {
    const std::string testDbPath = "/tmp/lingodb_test_rocksdb_static";
    cleanupTestDirectory(testDbPath);

    SECTION("databaseExists method") {
        // Test with non-existent database
        REQUIRE_FALSE(RocksDBStorage::databaseExists(testDbPath));
        
        // Create database
        auto status = RocksDBStorage::createDatabase(testDbPath);
        REQUIRE(status.ok());
        
        // Test with existing database
        REQUIRE(RocksDBStorage::databaseExists(testDbPath));
        
        cleanupTestDirectory(testDbPath);
        
        // Test with non-existent directory
        REQUIRE_FALSE(RocksDBStorage::databaseExists("/non/existent/path"));
    }

    SECTION("createDatabase error handling") {
        // Try to create database in invalid location (this should fail gracefully)
        auto status = RocksDBStorage::createDatabase("");
        REQUIRE_FALSE(status.ok());
    }

    cleanupTestDirectory(testDbPath);
}

TEST_CASE("RocksDBStorage: Large Data Operations", "[rocksdbs][large]") {
    const std::string testDbPath = "/tmp/lingodb_test_rocksdb_large";
    cleanupTestDirectory(testDbPath);
    
    auto status = RocksDBStorage::createDatabase(testDbPath);
    REQUIRE(status.ok());
    
    auto storage = std::make_shared<RocksDBStorage>(testDbPath);
    status = storage->open();
    REQUIRE(status.ok());

    SECTION("Large value operations") {
        // Test with very large values (1MB)
        std::string largeValue(1024 * 1024, 'X');
        std::string key = "large_value_key";
        
        status = storage->put(RocksDBStorage::ColumnFamily::TABLES, key, largeValue);
        REQUIRE(status.ok());
        
        std::string retrievedValue;
        status = storage->get(RocksDBStorage::ColumnFamily::TABLES, key, &retrievedValue);
        REQUIRE(status.ok());
        REQUIRE(retrievedValue == largeValue);
        REQUIRE(retrievedValue.size() == 1024 * 1024);
        
        // Clean up
        status = storage->del(RocksDBStorage::ColumnFamily::TABLES, key);
        REQUIRE(status.ok());
    }

    SECTION("Many keys operations") {
        const int numKeys = 1000;
        
        // Insert many keys
        for (int i = 0; i < numKeys; ++i) {
            std::string key = "many_keys_" + std::to_string(i);
            std::string value = "value_" + std::to_string(i);
            
            status = storage->put(RocksDBStorage::ColumnFamily::TABLES, key, value);
            REQUIRE(status.ok());
        }
        
        // Verify all keys exist
        for (int i = 0; i < numKeys; ++i) {
            std::string key = "many_keys_" + std::to_string(i);
            REQUIRE(storage->exists(RocksDBStorage::ColumnFamily::TABLES, key));
        }
        
        // Count keys using iterator
        auto iterator = storage->newIterator(RocksDBStorage::ColumnFamily::TABLES);
        int count = 0;
        for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
            std::string key = iterator->key().ToString();
            if (key.starts_with("many_keys_")) {
                count++;
            }
        }
        REQUIRE(count == numKeys);
        
        // Delete all keys using batch operation
        std::vector<std::tuple<RocksDBStorage::ColumnFamily, std::string, std::string>> deleteOps;
        for (int i = 0; i < numKeys; ++i) {
            std::string key = "many_keys_" + std::to_string(i);
            // Note: writeBatch doesn't support deletes directly, so delete individually
            status = storage->del(RocksDBStorage::ColumnFamily::TABLES, key);
            REQUIRE(status.ok());
        }
        
        // Verify all keys are deleted
        for (int i = 0; i < numKeys; ++i) {
            std::string key = "many_keys_" + std::to_string(i);
            REQUIRE_FALSE(storage->exists(RocksDBStorage::ColumnFamily::TABLES, key));
        }
    }

    storage->close();
    cleanupTestDirectory(testDbPath);
}

#endif // WITH_ROCKSDB 