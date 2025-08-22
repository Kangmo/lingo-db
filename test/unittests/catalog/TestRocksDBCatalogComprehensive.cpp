#include "catch2/catch_test_macros.hpp"

#ifdef WITH_ROCKSDB

#include "lingodb/catalog/RocksDBCatalog.h"
#include "lingodb/catalog/TableCatalogEntry.h"
#include "lingodb/runtime/storage/RocksDBStorage.h"
#include "lingodb/catalog/Defs.h"
#include "lingodb/catalog/Types.h"
#include "lingodb/catalog/Column.h"
#include "lingodb/utility/Serialization.h"
#include <filesystem>
#include <fstream>
#include <memory>

using namespace lingodb::catalog;
using namespace lingodb::runtime;
namespace fs = std::filesystem;

namespace {
    void cleanupTestDirectory(const std::string& path) {
        if (fs::exists(path)) {
            fs::remove_all(path);
        }
    }

    CreateTableDef createTestTableDef(const std::string& tableName) {
        CreateTableDef def;
        def.name = tableName;
        def.columns = {
            Column("id", Type::int64(), false),
            Column("name", Type::stringType(), true),
            Column("score", Type::f64(), true)
        };
        def.primaryKey = {"id"};
        return def;
    }

    CreateTableDef createComplexTableDef(const std::string& tableName) {
        CreateTableDef def;
        def.name = tableName;
        def.columns = {
            Column("id", Type::int64(), false),
            Column("name", Type::stringType(), true),
            Column("int8_col", Type::int8(), true),
            Column("int16_col", Type::int16(), true),
            Column("int32_col", Type::int32(), true),
            Column("float_col", Type::f32(), true),
            Column("double_col", Type::f64(), true),
            Column("bool_col", Type::boolean(), true)
        };
        def.primaryKey = {"id"};
        return def;
    }
}

TEST_CASE("RocksDBCatalog: Comprehensive Entry Management", "[catalog][rocksdb][entries]") {
    const std::string testDbPath = "/tmp/lingodb_test_rocksdb_catalog_entries";
    cleanupTestDirectory(testDbPath);
    
    {
        fs::create_directories(testDbPath);

        auto catalog = RocksDBCatalog::create(testDbPath, false);
        REQUIRE(catalog != nullptr);
        REQUIRE(catalog->hasRocksDBSupport());

        SECTION("Multiple entry insertions and retrievals") {
        const int numTables = 20;
        
        // Insert multiple table entries
        for (int i = 0; i < numTables; i++) {
            std::string tableName = "test_table_" + std::to_string(i);
            auto tableDef = createTestTableDef(tableName);
            
            auto storage = catalog->getRocksDBStorage();
            auto entry = RocksDBTableCatalogEntry::createFromCreateTable(tableDef, storage);
            catalog->insertEntry(entry);
        }
        
        // Verify all entries can be retrieved
        for (int i = 0; i < numTables; i++) {
            std::string tableName = "test_table_" + std::to_string(i);
            auto retrieved = catalog->getEntry(tableName);
            REQUIRE(retrieved.has_value());
            REQUIRE(retrieved.value()->getName() == tableName);
        }
        
        // Test getTypedEntry for all entries
        for (int i = 0; i < numTables; i++) {
            std::string tableName = "test_table_" + std::to_string(i);
            auto typed = catalog->getTypedEntry<RocksDBTableCatalogEntry>(tableName);
            REQUIRE(typed.has_value());
            REQUIRE(typed.value()->getName() == tableName);
        }
    }

    SECTION("Entry duplicate prevention") {
        std::string tableName = "duplicate_test_table";
        
        // Insert first entry
        auto tableDef1 = createTestTableDef(tableName);
        auto storage = catalog->getRocksDBStorage();
        auto entry1 = RocksDBTableCatalogEntry::createFromCreateTable(tableDef1, storage);
        catalog->insertEntry(entry1);
        
        auto retrieved1 = catalog->getEntry(tableName);
        REQUIRE(retrieved1.has_value());
        
        // Try to insert second entry with same name (should throw exception)
        auto tableDef2 = createComplexTableDef(tableName);
        auto entry2 = RocksDBTableCatalogEntry::createFromCreateTable(tableDef2, storage);
        REQUIRE_THROWS(catalog->insertEntry(entry2));
        
        // Verify first entry is still there
        auto retrieved2 = catalog->getEntry(tableName);
        REQUIRE(retrieved2.has_value());
        REQUIRE(retrieved2.value()->getName() == tableName);
        
        // Verify it's still the original table (3 columns, not 8)
        auto typedEntry = std::dynamic_pointer_cast<RocksDBTableCatalogEntry>(retrieved2.value());
        REQUIRE(typedEntry != nullptr);
        REQUIRE(typedEntry->getColumns().size() == 3); // Original table has 3 columns
    }

    SECTION("Non-existent entry retrieval variations") {
        // Test various non-existent names
        std::vector<std::string> nonExistentNames = {
            "does_not_exist",
            "",
            "very_long_table_name_that_definitely_does_not_exist_in_the_catalog",
            "table with spaces",
            "table-with-hyphens",
            "table.with.dots",
            "UPPERCASE_TABLE",
            "mixedCase_Table"
        };
        
        for (const auto& name : nonExistentNames) {
            auto result = catalog->getEntry(name);
            REQUIRE_FALSE(result.has_value());
            
            auto typedResult = catalog->getTypedEntry<RocksDBTableCatalogEntry>(name);
            REQUIRE_FALSE(typedResult.has_value());
        }
    }

        // Ensure catalog is properly destroyed before cleanup
        catalog.reset();
    }

    cleanupTestDirectory(testDbPath);
}

TEST_CASE("RocksDBCatalog: Serialization and Persistence", "[catalog][rocksdb][serialization]") {
    const std::string testDbPath = "/tmp/lingodb_test_rocksdb_catalog_serialization";
    cleanupTestDirectory(testDbPath);
    fs::create_directories(testDbPath);

    SECTION("Catalog serialization") {
        {
            auto catalog = RocksDBCatalog::create(testDbPath, false);
            REQUIRE(catalog != nullptr);
            
            // Add some entries
            for (int i = 0; i < 5; i++) {
                std::string tableName = "serialize_table_" + std::to_string(i);
                auto tableDef = createTestTableDef(tableName);
                auto storage = catalog->getRocksDBStorage();
                auto entry = RocksDBTableCatalogEntry::createFromCreateTable(tableDef, storage);
                catalog->insertEntry(entry);
            }
            
            // Test serialization
            lingodb::utility::SimpleByteWriter writer;
            lingodb::utility::Serializer serializer(writer);
            catalog->serialize(serializer);
            
            // Verify serializer has data
            REQUIRE(writer.size() > 0);
            
            // Ensure catalog is properly destroyed before cleanup
            catalog.reset();
        }
        
        cleanupTestDirectory(testDbPath);
    }

    SECTION("Persist and reload") {
        // Create catalog and add entries
        {
            auto catalog = RocksDBCatalog::create(testDbPath, false);
            REQUIRE(catalog != nullptr);
            
            auto tableDef = createComplexTableDef("persist_test_table");
            auto storage = catalog->getRocksDBStorage();
            auto entry = RocksDBTableCatalogEntry::createFromCreateTable(tableDef, storage);
            catalog->insertEntry(entry);
            
            // Explicit persist
            catalog->persist();
        }
        
        // Create new catalog instance and verify entry exists
        {
            auto newCatalog = RocksDBCatalog::create(testDbPath, false);
            REQUIRE(newCatalog != nullptr);
            
            auto retrieved = newCatalog->getEntry("persist_test_table");
            REQUIRE(retrieved.has_value());
            REQUIRE(retrieved.value()->getName() == "persist_test_table");
            
            // Verify it's the complex table with correct number of columns
            auto typedEntry = std::dynamic_pointer_cast<RocksDBTableCatalogEntry>(retrieved.value());
            REQUIRE(typedEntry != nullptr);
            REQUIRE(typedEntry->getColumns().size() == 8);
            
            // Ensure catalog is properly destroyed before cleanup
            newCatalog.reset();
        }
        
        cleanupTestDirectory(testDbPath);
    }

    cleanupTestDirectory(testDbPath);
}

TEST_CASE("RocksDBCatalog: Factory Methods", "[catalog][rocksdb][factory]") {
    const std::string testDbPath = "/tmp/lingodb_test_rocksdb_catalog_factory";
    cleanupTestDirectory(testDbPath);

    {
        SECTION("create method") {
            auto catalog = RocksDBCatalog::create(testDbPath, false);
            REQUIRE(catalog != nullptr);
            REQUIRE(catalog->hasRocksDBSupport());
            REQUIRE(catalog->getRocksDBStorage() != nullptr);
            REQUIRE(catalog->getDbDir() == testDbPath);
            
            // Ensure catalog is properly destroyed
            catalog.reset();
        }

        SECTION("create with eager loading") {
            // First create a catalog with some data
            {
                auto catalog = RocksDBCatalog::create(testDbPath, false);
                auto tableDef = createTestTableDef("eager_load_table");
                auto storage = catalog->getRocksDBStorage();
                auto entry = RocksDBTableCatalogEntry::createFromCreateTable(tableDef, storage);
                catalog->insertEntry(entry);
                catalog->persist();
            }
            
            // Create new catalog with eager loading
            {
                auto catalog = RocksDBCatalog::create(testDbPath, true);
                REQUIRE(catalog != nullptr);
                
                // Entry should be immediately available
                auto retrieved = catalog->getEntry("eager_load_table");
                REQUIRE(retrieved.has_value());
                
                // Ensure catalog is properly destroyed
                catalog.reset();
            }
        }

        SECTION("createEmpty method") {
            auto catalog = RocksDBCatalog::createEmpty();
            REQUIRE(catalog != nullptr);
            REQUIRE_FALSE(catalog->hasRocksDBSupport());
            REQUIRE(catalog->getRocksDBStorage() == nullptr);
            
            // Ensure catalog is properly destroyed
            catalog.reset();
        }
    }

    cleanupTestDirectory(testDbPath);
}

TEST_CASE("RocksDBCatalog: setShouldPersist", "[catalog][rocksdb][persist]") {
    const std::string testDbPath = "/tmp/lingodb_test_rocksdb_catalog_shouldpersist";
    cleanupTestDirectory(testDbPath);
    
    {
        fs::create_directories(testDbPath);

        auto catalog = RocksDBCatalog::create(testDbPath, false);
        REQUIRE(catalog != nullptr);

        SECTION("setShouldPersist functionality") {
            // Add an entry
            auto tableDef = createTestTableDef("should_persist_table");
            auto storage = catalog->getRocksDBStorage();
            auto entry = RocksDBTableCatalogEntry::createFromCreateTable(tableDef, storage);
            catalog->insertEntry(entry);
            
            // Test setShouldPersist
            catalog->setShouldPersist(true);
            catalog->setShouldPersist(false);
            
            // Should not crash or have side effects
            auto retrieved = catalog->getEntry("should_persist_table");
            REQUIRE(retrieved.has_value());
        }

        // Ensure catalog is properly destroyed before cleanup
        catalog.reset();
    }

    cleanupTestDirectory(testDbPath);
}

TEST_CASE("RocksDBCatalog: Complex Scenarios", "[catalog][rocksdb][complex]") {
    const std::string testDbPath = "/tmp/lingodb_test_rocksdb_catalog_complex";
    cleanupTestDirectory(testDbPath);
    
    {
        fs::create_directories(testDbPath);

        auto catalog = RocksDBCatalog::create(testDbPath, false);
        REQUIRE(catalog != nullptr);

    SECTION("Mixed entry types and operations") {
        // Add multiple types of entries with different characteristics
        std::vector<std::string> tableNames;
        
        // Simple tables
        for (int i = 0; i < 5; i++) {
            std::string name = "simple_table_" + std::to_string(i);
            tableNames.push_back(name);
            auto def = createTestTableDef(name);
            auto storage = catalog->getRocksDBStorage();
            auto entry = RocksDBTableCatalogEntry::createFromCreateTable(def, storage);
            catalog->insertEntry(entry);
        }
        
        // Complex tables
        for (int i = 0; i < 3; i++) {
            std::string name = "complex_table_" + std::to_string(i);
            tableNames.push_back(name);
            auto def = createComplexTableDef(name);
            auto storage = catalog->getRocksDBStorage();
            auto entry = RocksDBTableCatalogEntry::createFromCreateTable(def, storage);
            catalog->insertEntry(entry);
        }
        
        // Verify all entries
        for (const auto& name : tableNames) {
            auto retrieved = catalog->getEntry(name);
            REQUIRE(retrieved.has_value());
            REQUIRE(retrieved.value()->getName() == name);
        }
        
        // Test persistence with mixed entries
        catalog->persist();
        
        // Verify entries survive persistence
        for (const auto& name : tableNames) {
            auto retrieved = catalog->getEntry(name);
            REQUIRE(retrieved.has_value());
        }
    }

    SECTION("Large number of entries") {
        const int numEntries = 100;
        
        // Add many entries
        for (int i = 0; i < numEntries; i++) {
            std::string name = "large_scale_table_" + std::to_string(i);
            auto def = (i % 2 == 0) ? createTestTableDef(name) : createComplexTableDef(name);
            auto storage = catalog->getRocksDBStorage();
            auto entry = RocksDBTableCatalogEntry::createFromCreateTable(def, storage);
            catalog->insertEntry(entry);
        }
        
        // Verify all entries
        for (int i = 0; i < numEntries; i++) {
            std::string name = "large_scale_table_" + std::to_string(i);
            auto retrieved = catalog->getEntry(name);
            REQUIRE(retrieved.has_value());
            REQUIRE(retrieved.value()->getName() == name);
        }
        
        // Test persistence with many entries
        catalog->persist();
    }

        // Explicitly reset catalog before cleanup
        catalog.reset();
    } // Ensure all objects are destroyed before cleanup

    cleanupTestDirectory(testDbPath);
}

TEST_CASE("RocksDBCatalog: Special Characters and Edge Cases", "[catalog][rocksdb][edge]") {
    const std::string testDbPath = "/tmp/lingodb_test_rocksdb_catalog_edge";
    cleanupTestDirectory(testDbPath);
    
    {
        fs::create_directories(testDbPath);

        auto catalog = RocksDBCatalog::create(testDbPath, false);
        REQUIRE(catalog != nullptr);

    SECTION("Tables with special characters in names") {
        std::vector<std::string> specialNames = {
            "table_with_underscores",
            "table-with-hyphens",
            "table.with.dots",
            "UPPERCASE_TABLE",
            "mixedCase_Table",
            "table123_with_numbers",
            "very_long_table_name_with_many_characters_to_test_length_limits"
        };
        
        for (const auto& name : specialNames) {
            auto def = createTestTableDef(name);
            auto storage = catalog->getRocksDBStorage();
            auto entry = RocksDBTableCatalogEntry::createFromCreateTable(def, storage);
            catalog->insertEntry(entry);
            
            // Verify immediate retrieval
            auto retrieved = catalog->getEntry(name);
            REQUIRE(retrieved.has_value());
            REQUIRE(retrieved.value()->getName() == name);
        }
        
        // Verify all entries persist
        catalog->persist();
        
        for (const auto& name : specialNames) {
            auto retrieved = catalog->getEntry(name);
            REQUIRE(retrieved.has_value());
            REQUIRE(retrieved.value()->getName() == name);
        }
    }

    SECTION("Empty and whitespace handling") {
        // Note: Empty string table names might not be allowed by the schema,
        // but we can test the catalog's handling
        
        // Test retrieval of empty string (should fail gracefully)
        auto emptyResult = catalog->getEntry("");
        REQUIRE_FALSE(emptyResult.has_value());
        
        // Test with whitespace-only names (if they're allowed by validation)
        // This would depend on how the catalog validates table names
    }

        // Explicitly reset catalog before cleanup
        catalog.reset();
    } // Ensure all objects are destroyed before cleanup

    cleanupTestDirectory(testDbPath);
}

#endif // WITH_ROCKSDB 