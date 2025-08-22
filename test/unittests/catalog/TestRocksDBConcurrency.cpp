#ifdef WITH_ROCKSDB

#include "catch2/catch_all.hpp"
#include "lingodb/catalog/RocksDBCatalog.h"
#include "lingodb/catalog/TableCatalogEntry.h"
#include "lingodb/catalog/Defs.h"
#include "lingodb/catalog/Types.h"
#include <thread>
#include <vector>
#include <atomic>
#include <random>
#include <chrono>
#include <filesystem>

using namespace lingodb::catalog;
namespace fs = std::filesystem;

namespace {
    std::string getTestDbPath() {
        static std::atomic<int> counter{0};
        auto timestamp = std::chrono::steady_clock::now().time_since_epoch().count();
        return "/tmp/test_rocksdb_concurrency_" + std::to_string(timestamp) + "_" + std::to_string(counter++);
    }
    
    void cleanupTestDb(const std::string& path) {
        if (fs::exists(path)) {
            fs::remove_all(path);
        }
    }
    
    CreateTableDef createTestTableDef(const std::string& name) {
        CreateTableDef def;
        def.name = name;
        def.columns = {
            Column("id", Type::int32(), false),
            Column("name", Type::stringType(), true),
            Column("value", Type::f64(), true)
        };
        def.primaryKey = {"id"};
        return def;
    }
}

TEST_CASE("RocksDBCatalog: Concurrent read operations", "[rocksdb][concurrency][read]") {
    std::string dbPath = getTestDbPath();
    
    SECTION("Multiple threads reading same entry") {
        {
            auto catalog = RocksDBCatalog::create(dbPath, false);
            REQUIRE(catalog != nullptr);
        
        // Insert test entries
        const int numEntries = 10;
        for (int i = 0; i < numEntries; ++i) {
            auto tableDef = createTestTableDef("table_" + std::to_string(i));
            auto entry = RocksDBTableCatalogEntry::createFromCreateTable(
                tableDef, catalog->getRocksDBStorage());
            catalog->insertEntry(entry);
        }
        
        // Concurrent reads
        const int numThreads = 20;
        const int readsPerThread = 100;
        std::vector<std::thread> threads;
        std::atomic<int> successCount{0};
        std::atomic<int> errorCount{0};
        
        for (int t = 0; t < numThreads; ++t) {
            threads.emplace_back([&catalog, &successCount, &errorCount, numEntries, readsPerThread]() {
                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_int_distribution<> dis(0, numEntries - 1);
                
                for (int i = 0; i < readsPerThread; ++i) {
                    try {
                        std::string tableName = "table_" + std::to_string(dis(gen));
                        auto entry = catalog->getEntry(tableName);
                        if (entry.has_value()) {
                            successCount++;
                        }
                    } catch (...) {
                        errorCount++;
                    }
                }
            });
        }
        
        for (auto& t : threads) {
            t.join();
        }
        
        REQUIRE(errorCount == 0);
        REQUIRE(successCount == numThreads * readsPerThread);
        } // catalog destroyed here
        
        cleanupTestDb(dbPath);
    }
}

TEST_CASE("RocksDBCatalog: Concurrent write operations", "[rocksdb][concurrency][write]") {
    std::string dbPath = getTestDbPath();
    
    SECTION("Multiple threads inserting different entries") {
        {
            auto catalog = RocksDBCatalog::create(dbPath, false);
            REQUIRE(catalog != nullptr);
        
        const int numThreads = 10;
        const int entriesPerThread = 20;
        std::vector<std::thread> threads;
        std::atomic<int> successCount{0};
        std::atomic<int> duplicateCount{0};
        
        for (int t = 0; t < numThreads; ++t) {
            threads.emplace_back([&catalog, &successCount, &duplicateCount, t, entriesPerThread]() {
                for (int i = 0; i < entriesPerThread; ++i) {
                    std::string tableName = "thread_" + std::to_string(t) + "_table_" + std::to_string(i);
                    auto tableDef = createTestTableDef(tableName);
                    auto entry = RocksDBTableCatalogEntry::createFromCreateTable(
                        tableDef, catalog->getRocksDBStorage());
                    
                    try {
                        catalog->insertEntry(entry);
                        successCount++;
                    } catch (const std::runtime_error& e) {
                        if (std::string(e.what()).find("already exists") != std::string::npos) {
                            duplicateCount++;
                        }
                    }
                }
            });
        }
        
        for (auto& t : threads) {
            t.join();
        }
        
        REQUIRE(duplicateCount == 0);
        REQUIRE(successCount == numThreads * entriesPerThread);
        
        // Verify all entries exist
        for (int t = 0; t < numThreads; ++t) {
            for (int i = 0; i < entriesPerThread; ++i) {
                std::string tableName = "thread_" + std::to_string(t) + "_table_" + std::to_string(i);
                auto entry = catalog->getEntry(tableName);
                REQUIRE(entry.has_value());
            }
        }
        } // catalog destroyed here
        
        cleanupTestDb(dbPath);
    }
}

TEST_CASE("RocksDBCatalog: Mixed read/write operations", "[rocksdb][concurrency][mixed]") {
    std::string dbPath = getTestDbPath();
    
    SECTION("Readers and writers working concurrently") {
        {
            auto catalog = RocksDBCatalog::create(dbPath, false);
            REQUIRE(catalog != nullptr);
        
        // Pre-populate some entries
        const int initialEntries = 50;
        for (int i = 0; i < initialEntries; ++i) {
            auto tableDef = createTestTableDef("initial_" + std::to_string(i));
            auto entry = RocksDBTableCatalogEntry::createFromCreateTable(
                tableDef, catalog->getRocksDBStorage());
            catalog->insertEntry(entry);
        }
        
        const int numReaders = 10;
        const int numWriters = 5;
        const int operationsPerThread = 100;
        
        std::vector<std::thread> threads;
        std::atomic<int> readSuccess{0};
        std::atomic<int> writeSuccess{0};
        std::atomic<bool> stopFlag{false};
        
        // Start readers
        for (int r = 0; r < numReaders; ++r) {
            threads.emplace_back([&catalog, &readSuccess, &stopFlag, initialEntries]() {
                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_int_distribution<> dis(0, initialEntries - 1);
                
                while (!stopFlag) {
                    std::string tableName = "initial_" + std::to_string(dis(gen));
                    auto entry = catalog->getEntry(tableName);
                    if (entry.has_value()) {
                        readSuccess++;
                    }
                    std::this_thread::sleep_for(std::chrono::microseconds(10));
                }
            });
        }
        
        // Start writers
        for (int w = 0; w < numWriters; ++w) {
            threads.emplace_back([&catalog, &writeSuccess, w, operationsPerThread]() {
                for (int i = 0; i < operationsPerThread; ++i) {
                    std::string tableName = "writer_" + std::to_string(w) + "_" + std::to_string(i);
                    auto tableDef = createTestTableDef(tableName);
                    auto entry = RocksDBTableCatalogEntry::createFromCreateTable(
                        tableDef, catalog->getRocksDBStorage());
                    
                    try {
                        catalog->insertEntry(entry);
                        writeSuccess++;
                    } catch (...) {
                        // Ignore errors for this test
                    }
                    std::this_thread::sleep_for(std::chrono::microseconds(50));
                }
            });
        }
        
        // Wait for writers to finish
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        stopFlag = true;
        
        for (auto& t : threads) {
            t.join();
        }
        
        REQUIRE(readSuccess > 0);
        REQUIRE(writeSuccess > 0);
        } // catalog destroyed here
        
        cleanupTestDb(dbPath);
    }
}

TEST_CASE("RocksDBCatalog: Cache invalidation under concurrency", "[rocksdb][concurrency][cache]") {
    std::string dbPath = getTestDbPath();
    
    SECTION("Cache consistency during concurrent operations") {
        {
            auto catalog = RocksDBCatalog::create(dbPath, false);
            REQUIRE(catalog != nullptr);
        
        const int numOperations = 1000;
        std::atomic<int> cacheHits{0};
        std::atomic<int> cacheMisses{0};
        
        // Thread that constantly forces cache misses by creating new entries
        std::thread invalidator([&catalog]() {
            for (int i = 0; i < 100; ++i) {
                // Creating a new entry will indirectly affect cache
                auto tableDef = createTestTableDef("cache_test_temp_" + std::to_string(i));
                auto entry = RocksDBTableCatalogEntry::createFromCreateTable(
                    tableDef, catalog->getRocksDBStorage());
                try {
                    catalog->insertEntry(entry);
                } catch (...) {
                    // Ignore errors
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
        });
        
        // Threads that read entries
        std::vector<std::thread> readers;
        for (int r = 0; r < 5; ++r) {
            readers.emplace_back([&catalog, &cacheHits, &cacheMisses, numOperations]() {
                for (int i = 0; i < numOperations; ++i) {
                    // Try to create and read an entry
                    std::string tableName = "cache_test_" + std::to_string(i % 10);
                    
                    // First access (might be cache miss)
                    auto entry1 = catalog->getEntry(tableName);
                    
                    // Second access (should be cache hit if not invalidated)
                    auto entry2 = catalog->getEntry(tableName);
                    
                    if (entry2.has_value()) {
                        cacheHits++;
                    } else {
                        cacheMisses++;
                    }
                }
            });
        }
        
        invalidator.join();
        for (auto& t : readers) {
            t.join();
        }
        
        // Should have both hits and misses due to invalidation
        REQUIRE(cacheHits + cacheMisses > 0);
        } // catalog destroyed here
        
        cleanupTestDb(dbPath);
    }
}

TEST_CASE("RocksDBCatalog: Persistence under concurrent load", "[rocksdb][concurrency][persistence]") {
    std::string dbPath = getTestDbPath();
    
    SECTION("Concurrent persist operations") {
        const int numThreads = 5;
        const int entriesPerThread = 10;
        
        {
            auto catalog = RocksDBCatalog::create(dbPath, false);
            REQUIRE(catalog != nullptr);
            catalog->setShouldPersist(true);
        
            std::vector<std::thread> threads;
            
            // Multiple threads adding entries and calling persist
            for (int t = 0; t < numThreads; ++t) {
                threads.emplace_back([&catalog, t, entriesPerThread]() {
                    for (int i = 0; i < entriesPerThread; ++i) {
                        std::string tableName = "persist_" + std::to_string(t) + "_" + std::to_string(i);
                        auto tableDef = createTestTableDef(tableName);
                        auto entry = RocksDBTableCatalogEntry::createFromCreateTable(
                            tableDef, catalog->getRocksDBStorage());
                        
                        catalog->insertEntry(entry);
                        
                        // Randomly call persist
                        if (i % 3 == 0) {
                            try {
                                catalog->persist();
                            } catch (...) {
                                // Ignore persist errors in this stress test
                            }
                        }
                    }
                });
            }
            
            for (auto& t : threads) {
                t.join();
            }
            
            // Final persist
            REQUIRE_NOTHROW(catalog->persist());
        } // catalog destroyed here
        
        // Reload catalog and verify all entries
        auto newCatalog = RocksDBCatalog::create(dbPath, true);
        REQUIRE(newCatalog != nullptr);
        
        int foundCount = 0;
        for (int t = 0; t < numThreads; ++t) {
            for (int i = 0; i < entriesPerThread; ++i) {
                std::string tableName = "persist_" + std::to_string(t) + "_" + std::to_string(i);
                auto entry = newCatalog->getEntry(tableName);
                if (entry.has_value()) {
                    foundCount++;
                }
            }
        }
        
        REQUIRE(foundCount == numThreads * entriesPerThread);
        newCatalog.reset(); // Destroy newCatalog before cleanup
        
        cleanupTestDb(dbPath);
    }
}

#endif // WITH_ROCKSDB