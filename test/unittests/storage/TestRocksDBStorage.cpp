#include "lingodb/runtime/storage/RocksDBStorage.h"
#include <arrow/api.h>
#include <arrow/ipc/json_simple.h>
#include <arrow/ipc/reader.h>
#include <arrow/table.h>
#include <catch2/catch_all.hpp>
#include <filesystem>

using namespace lingodb::runtime;
using namespace std;
namespace fs = std::filesystem;

TEST_CASE("RocksDBStorage basic create/append/get", "[RocksDBStorage]") {
    fs::remove_all("test_table.rocksdb");
    // Define schema
    arrow::FieldVector fields = {
        arrow::field("id", arrow::int64()),
        arrow::field("value", arrow::utf8())
    };
    auto schema = std::make_shared<arrow::Schema>(fields);

    // Create storage
    auto storage = std::make_unique<RocksDBStorage>("test_table.rocksdb", schema);

    // Create Arrow RecordBatch
    arrow::Int64Builder id_builder;
    arrow::StringBuilder value_builder;
    REQUIRE(id_builder.Append(1).ok());
    REQUIRE(id_builder.Append(2).ok());
    REQUIRE(value_builder.Append("foo").ok());
    REQUIRE(value_builder.Append("bar").ok());
    std::shared_ptr<arrow::Array> id_array;
    std::shared_ptr<arrow::Array> value_array;
    REQUIRE(id_builder.Finish(&id_array).ok());
    REQUIRE(value_builder.Finish(&value_array).ok());
    std::vector<std::shared_ptr<arrow::Array>> arrays = {id_array, value_array};
    auto batch = arrow::RecordBatch::Make(schema, 2, arrays);

    // Append batch
    storage->append({batch});

    // Ensure the number of rows is correct
    REQUIRE(storage->getNumRows() == 2);

    // Clean up
    fs::remove_all("test_table.rocksdb");
}
