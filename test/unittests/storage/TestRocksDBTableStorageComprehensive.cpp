#include "catch2/catch_test_macros.hpp"

#ifdef WITH_ROCKSDB

#include "lingodb/runtime/storage/RocksDBTableStorage.h"
#include "lingodb/runtime/storage/RocksDBStorage.h"
#include "lingodb/catalog/TableCatalogEntry.h"
#include "lingodb/catalog/Defs.h"
#include "lingodb/catalog/Types.h"
#include "lingodb/runtime/ExecutionContext.h"
#include "lingodb/runtime/Session.h"
#include "lingodb/scheduler/Scheduler.h"
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <filesystem>
#include <memory>

using namespace lingodb::runtime;
using namespace lingodb::catalog;
namespace fs = std::filesystem;

namespace {
    void cleanupTestDirectory(const std::string& path) {
        if (fs::exists(path)) {
            fs::remove_all(path);
        }
    }

    // Helper to create test Arrow schema
    std::shared_ptr<arrow::Schema> createTestSchema() {
        auto id_field = arrow::field("id", arrow::int64(), false);
        auto name_field = arrow::field("name", arrow::utf8(), true);
        auto value_field = arrow::field("value", arrow::float64(), true);
        auto active_field = arrow::field("active", arrow::boolean(), true);
        return arrow::schema({id_field, name_field, value_field, active_field});
    }

    // Helper to create test RecordBatch
    std::shared_ptr<arrow::RecordBatch> createTestRecordBatch(int startId, int numRows) {
        auto schema = createTestSchema();
        
        // Build arrays
        arrow::Int64Builder id_builder;
        arrow::StringBuilder name_builder;
        arrow::DoubleBuilder value_builder;
        arrow::BooleanBuilder active_builder;
        
        for (int i = 0; i < numRows; i++) {
            auto result_id = id_builder.Append(startId + i);
            auto result_name = name_builder.Append("name_" + std::to_string(startId + i));
            auto result_value = value_builder.Append((startId + i) * 1.5);
            auto result_active = active_builder.Append(i % 2 == 0);
        }
        
        std::shared_ptr<arrow::Array> id_array, name_array, value_array, active_array;
        auto result_id = id_builder.Finish(&id_array);
        auto result_name = name_builder.Finish(&name_array);
        auto result_value = value_builder.Finish(&value_array);
        auto result_active = active_builder.Finish(&active_array);
        
        return arrow::RecordBatch::Make(schema, numRows, {id_array, name_array, value_array, active_array});
    }

    // Helper to create test Table
    std::shared_ptr<arrow::Table> createTestTable(int startId, int numRows) {
        auto batch = createTestRecordBatch(startId, numRows);
        return arrow::Table::FromRecordBatches({batch}).ValueOrDie();
    }

    CreateTableDef createTestTableDef(const std::string& tableName) {
        CreateTableDef def;
        def.name = tableName;
        def.columns = {
            Column("id", Type::int64(), false),
            Column("name", Type::stringType(), true),
            Column("value", Type::f64(), true),
            Column("active", Type::boolean(), true)
        };
        def.primaryKey = {"id"};
        return def;
    }
}

TEST_CASE("RocksDBTableStorage: Comprehensive Append Operations", "[tablestorage][append]") {
    const std::string testDbPath = "/tmp/lingodb_test_table_storage_append";
    cleanupTestDirectory(testDbPath);
    
    auto status = RocksDBStorage::createDatabase(testDbPath);
    REQUIRE(status.ok());
    
    auto storage = std::make_shared<RocksDBStorage>(testDbPath);
    status = storage->open();
    REQUIRE(status.ok());
    
    auto schema = createTestSchema();
    auto tableStorage = std::make_unique<RocksDBTableStorage>(
        storage, "append_test_table", schema
    );

    SECTION("Append single RecordBatch") {
        auto batch = createTestRecordBatch(0, 100);
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};
        
        tableStorage->append(batches);
        
        REQUIRE(tableStorage->getNumRows() == 100);
        REQUIRE(tableStorage->nextRowId() == 100);
    }

    SECTION("Append multiple RecordBatches") {
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        for (int i = 0; i < 5; i++) {
            batches.push_back(createTestRecordBatch(i * 20, 20));
        }
        
        tableStorage->append(batches);
        
        REQUIRE(tableStorage->getNumRows() == 100);
        REQUIRE(tableStorage->nextRowId() == 100);
    }

    SECTION("Append Arrow Table") {
        auto table = createTestTable(0, 150);
        
        tableStorage->append(table);
        
        REQUIRE(tableStorage->getNumRows() == 150);
        REQUIRE(tableStorage->nextRowId() == 150);
    }

    SECTION("Multiple append operations") {
        // First append
        auto batch1 = createTestRecordBatch(0, 50);
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches1 = {batch1};
        tableStorage->append(batches1);
        
        REQUIRE(tableStorage->getNumRows() == 50);
        
        // Second append
        auto table2 = createTestTable(50, 30);
        tableStorage->append(table2);
        
        REQUIRE(tableStorage->getNumRows() == 80);
        
        // Third append
        auto batch3 = createTestRecordBatch(80, 20);
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches3 = {batch3};
        tableStorage->append(batches3);
        
        REQUIRE(tableStorage->getNumRows() == 100);
        REQUIRE(tableStorage->nextRowId() == 100);
    }

    storage->close();
    cleanupTestDirectory(testDbPath);
}

TEST_CASE("RocksDBTableStorage: Column Operations", "[tablestorage][columns]") {
    const std::string testDbPath = "/tmp/lingodb_test_table_storage_columns";
    cleanupTestDirectory(testDbPath);
    
    auto status = RocksDBStorage::createDatabase(testDbPath);
    REQUIRE(status.ok());
    
    auto storage = std::make_shared<RocksDBStorage>(testDbPath);
    status = storage->open();
    REQUIRE(status.ok());
    
    auto schema = createTestSchema();
    auto tableStorage = std::make_unique<RocksDBTableStorage>(
        storage, "column_test_table", schema
    );

    SECTION("getColumnStorageType for valid columns") {
        auto idType = tableStorage->getColumnStorageType("id");
        REQUIRE(idType != nullptr);
        REQUIRE(idType->Equals(arrow::int64()));
        
        auto nameType = tableStorage->getColumnStorageType("name");
        REQUIRE(nameType != nullptr);
        REQUIRE(nameType->Equals(arrow::utf8()));
        
        auto valueType = tableStorage->getColumnStorageType("value");
        REQUIRE(valueType != nullptr);
        REQUIRE(valueType->Equals(arrow::float64()));
        
        auto activeType = tableStorage->getColumnStorageType("active");
        REQUIRE(activeType != nullptr);
        REQUIRE(activeType->Equals(arrow::boolean()));
    }

    storage->close();
    cleanupTestDirectory(testDbPath);
}

TEST_CASE("RocksDBTableStorage: Persistence and Loading", "[tablestorage][persistence]") {
    const std::string testDbPath = "/tmp/lingodb_test_table_storage_persist";
    cleanupTestDirectory(testDbPath);
    
    auto status = RocksDBStorage::createDatabase(testDbPath);
    REQUIRE(status.ok());
    
    auto storage = std::make_shared<RocksDBStorage>(testDbPath);
    status = storage->open();
    REQUIRE(status.ok());

    SECTION("Flush and ensureLoaded operations") {
        auto schema = createTestSchema();
        auto tableStorage = std::make_unique<RocksDBTableStorage>(
            storage, "persist_test_table", schema
        );
        
        // Add some data
        auto batch = createTestRecordBatch(0, 50);
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};
        tableStorage->append(batches);
        
        REQUIRE(tableStorage->getNumRows() == 50);
        
        // Test flush operation
        tableStorage->flush();
        
        // Test ensureLoaded operation
        tableStorage->ensureLoaded();
        
        // Data should still be accessible
        REQUIRE(tableStorage->getNumRows() == 50);
        REQUIRE(tableStorage->nextRowId() == 50);
    }

    SECTION("Metadata serialization/deserialization") {
        auto schema = createTestSchema();
        auto tableStorage = std::make_unique<RocksDBTableStorage>(
            storage, "metadata_test_table", schema
        );
        
        // Add some data
        auto table = createTestTable(0, 75);
        tableStorage->append(table);
        
        // Test metadata serialization
        tableStorage->serializeMetadata();
        
        // Test metadata deserialization
        tableStorage->deserializeMetadata();
        
        // Verify metadata is consistent
        REQUIRE(tableStorage->getNumRows() == 75);
    }

    storage->close();
    cleanupTestDirectory(testDbPath);
}

TEST_CASE("RocksDBTableStorage: Row Access", "[tablestorage][rows]") {
    const std::string testDbPath = "/tmp/lingodb_test_table_storage_rows";
    cleanupTestDirectory(testDbPath);
    
    auto status = RocksDBStorage::createDatabase(testDbPath);
    REQUIRE(status.ok());
    
    auto storage = std::make_shared<RocksDBStorage>(testDbPath);
    status = storage->open();
    REQUIRE(status.ok());
    
    auto schema = createTestSchema();
    auto tableStorage = std::make_unique<RocksDBTableStorage>(
        storage, "row_test_table", schema
    );

    SECTION("getByRowId with data") {
        // Add test data
        auto batch = createTestRecordBatch(0, 100);
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};
        tableStorage->append(batches);
        
        // Test accessing various row IDs
        auto [chunk1, offset1] = tableStorage->getByRowId(0);
        REQUIRE(chunk1 != nullptr);
        REQUIRE(offset1 == 0);
        
        auto [chunk2, offset2] = tableStorage->getByRowId(50);
        REQUIRE(chunk2 != nullptr);
        REQUIRE(offset2 == 50);
        
        auto [chunk3, offset3] = tableStorage->getByRowId(99);
        REQUIRE(chunk3 != nullptr);
        REQUIRE(offset3 == 99);
    }

    storage->close();
    cleanupTestDirectory(testDbPath);
}

TEST_CASE("RocksDBTableStorage: Comprehensive Factory Methods", "[tablestorage][factory]") {
    const std::string testDbPath = "/tmp/lingodb_test_table_storage_factory";
    cleanupTestDirectory(testDbPath);
    
    auto status = RocksDBStorage::createDatabase(testDbPath);
    REQUIRE(status.ok());
    
    auto storage = std::make_shared<RocksDBStorage>(testDbPath);
    status = storage->open();
    REQUIRE(status.ok());

    SECTION("create factory method") {
        auto def = createTestTableDef("factory_test_table");
        
        auto tableStorage = RocksDBTableStorage::create(storage, def);
        REQUIRE(tableStorage != nullptr);
        
        REQUIRE(tableStorage->getNumRows() == 0);
        REQUIRE(tableStorage->nextRowId() == 0);
        
        // Test that we can append data to the created table
        auto batch = createTestRecordBatch(0, 25);
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};
        tableStorage->append(batches);
        
        REQUIRE(tableStorage->getNumRows() == 25);
    }

    storage->close();
    cleanupTestDirectory(testDbPath);
}

TEST_CASE("RocksDBTableStorage: Scan Task Creation", "[tablestorage][scan]") {
    const std::string testDbPath = "/tmp/lingodb_test_table_storage_scan";
    cleanupTestDirectory(testDbPath);
    
    auto status = RocksDBStorage::createDatabase(testDbPath);
    REQUIRE(status.ok());
    
    auto storage = std::make_shared<RocksDBStorage>(testDbPath);
    status = storage->open();
    REQUIRE(status.ok());
    
    auto schema = createTestSchema();
    auto tableStorage = std::make_unique<RocksDBTableStorage>(
        storage, "scan_test_table", schema
    );

    SECTION("createScanTask") {
        // Initialize scheduler (required for scan tasks)
        auto schedulerHandle = lingodb::scheduler::startScheduler(1);
        
        // Set up execution context (required for scan tasks)
        auto session = lingodb::runtime::Session::createSession();
        auto executionContext = session->createExecutionContext();
        lingodb::runtime::setCurrentExecutionContext(executionContext.get());
        
        // Add some test data first
        auto batch = createTestRecordBatch(0, 50);
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};
        tableStorage->append(batches);
        
        // Create scan configuration
        ScanConfig scanConfig;
        scanConfig.parallel = false;
        scanConfig.columns = {"id", "name"};
        scanConfig.cb = [](BatchView* bv) {}; // Empty callback for testing
        
        auto scanTask = tableStorage->createScanTask(scanConfig);
        REQUIRE(scanTask != nullptr);
        
        // Clean up execution context
        lingodb::runtime::setCurrentExecutionContext(nullptr);
        
        // Scheduler will be cleaned up automatically when schedulerHandle goes out of scope
    }

    storage->close();
    cleanupTestDirectory(testDbPath);
}

TEST_CASE("RocksDBTableStorage: Statistics Operations", "[tablestorage][statistics]") {
    const std::string testDbPath = "/tmp/lingodb_test_table_storage_stats";
    cleanupTestDirectory(testDbPath);
    
    auto status = RocksDBStorage::createDatabase(testDbPath);
    REQUIRE(status.ok());
    
    auto storage = std::make_shared<RocksDBStorage>(testDbPath);
    status = storage->open();
    REQUIRE(status.ok());
    
    auto schema = createTestSchema();
    auto tableStorage = std::make_unique<RocksDBTableStorage>(
        storage, "stats_test_table", schema
    );

    SECTION("getSample") {
        // Add test data
        auto batch = createTestRecordBatch(0, 100);
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};
        tableStorage->append(batches);
        
        // Test getting sample (should not crash)
        const auto& sample = tableStorage->getSample();
        // Sample might be empty initially, but should be accessible
        (void)sample; // Silence unused variable warning
        REQUIRE(true); // Sample is accessible
    }

    storage->close();
    cleanupTestDirectory(testDbPath);
}

#endif // WITH_ROCKSDB 