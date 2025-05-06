#include "lingodb/runtime/storage/RocksDBStorage.h"
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>
#include <arrow/compute/api.h>
#include <sstream>
#include <stdexcept>
#include <iostream>

namespace lingodb::runtime {

RocksDBStorage::TableChunk::TableChunk(std::shared_ptr<arrow::RecordBatch> data, size_t startRowId)
    : internalData(data), startRowId(startRowId), numRows(data->num_rows()) {
   for (auto colId = 0; colId < data->num_columns(); colId++) {
      columnInfo.push_back(RecordBatchInfo::getColumnInfo(colId, data));
   }
}

std::unique_ptr<RocksDBStorage> RocksDBStorage::create(const catalog::CreateTableDef& def) {
    arrow::FieldVector fields;
    for (auto& c : def.columns) {
        fields.push_back(std::make_shared<arrow::Field>(c.getColumnName(), toPhysicalType(c.getLogicalType())));
    }
    auto arrowSchema = std::make_shared<arrow::Schema>(fields);
    std::string db_path = def.name + ".rocksdb";
    return std::make_unique<RocksDBStorage>(db_path, arrowSchema);
}

RocksDBStorage::RocksDBStorage(const std::string& db_path, std::shared_ptr<arrow::Schema> schema)
    : db_path(db_path), schema(schema), sample(std::shared_ptr<arrow::RecordBatch>()), numRows(0), persist(true) {
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB* db_ptr = nullptr;
    auto status = rocksdb::DB::Open(options, db_path, &db_ptr);
    if (!status.ok()) {
        throw std::runtime_error("Failed to open RocksDB: " + status.ToString());
    }
    db.reset(db_ptr);
    
    // Initialize column statistics
    for (auto c : schema->fields()) {
        columnStatistics[c->name()] = catalog::ColumnStatistics(std::nullopt);
    }
    
    loadSchemaAndStats();
}

void RocksDBStorage::append(const std::shared_ptr<arrow::Table>& table) {
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    arrow::TableBatchReader reader(table);
    std::shared_ptr<arrow::RecordBatch> nextChunk;
    while (reader.ReadNext(&nextChunk) == arrow::Status::OK()) {
        if (nextChunk) {
            batches.push_back(nextChunk);
        } else {
            break;
        }
        nextChunk.reset();
    }
    append(batches);
}

void RocksDBStorage::append(const std::vector<std::shared_ptr<arrow::RecordBatch>>& toAppend) {
    rocksdb::WriteBatch batch;
    
    for (auto& record_batch : toAppend) {
        if (!record_batch->schema()->Equals(*schema)) {
            std::cout << "schema to add: " << record_batch->schema()->ToString() << std::endl;
            std::cout << "schema of table: " << schema->ToString() << std::endl;
            throw std::runtime_error("schema mismatch");
        }
        
        // Serialize RecordBatch
        arrow::ipc::IpcWriteOptions opts = arrow::ipc::IpcWriteOptions::Defaults();
        auto result = arrow::ipc::SerializeRecordBatch(*record_batch, opts);
        if (!result.ok()) {
            throw std::runtime_error("Failed to serialize RecordBatch for RocksDB: " + result.status().ToString());
        }
        
        std::shared_ptr<arrow::Buffer> buffer = result.ValueOrDie();
        
        // Store the batch with key = "batch_" + start_row_id
        std::string key = "batch_" + std::to_string(numRows);
        batch.Put(key, buffer->ToString());
        
        // Add to in-memory table chunks
        tableData.push_back(TableChunk{record_batch, numRows});
        numRows += record_batch->num_rows();
    }
    
    auto status = db->Write(rocksdb::WriteOptions(), &batch);
    if (!status.ok()) {
        throw std::runtime_error("Failed to write batch to RocksDB: " + status.ToString());
    }
    
    // Update statistics
    auto tableView = arrow::Table::FromRecordBatches(toAppend).ValueOrDie();
    for (auto c : schema->fields()) {
        columnStatistics[c->name()] = catalog::ColumnStatistics(countDistinctValues(tableView->GetColumnByName(c->name())));
    }
    
    storeSchemaAndStats();
}

void RocksDBStorage::flush() {
    if (!persist) return;
    storeSchemaAndStats();
    // Could also trigger a manual compaction if needed
    // db->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr);
}

const catalog::ColumnStatistics& RocksDBStorage::getColumnStatistics(const std::string& column) const {
    auto it = columnStatistics.find(column);
    if (it == columnStatistics.end()) {
        throw std::runtime_error("Column not found in statistics");
    }
    return it->second;
}

void RocksDBStorage::ensureLoaded() {
    // If tableData is empty but we have data in RocksDB, load it
    if (tableData.empty() && numRows > 0) {
        loadTableData();
    }
}

std::pair<const RocksDBStorage::TableChunk*, size_t> RocksDBStorage::getByRowId(size_t rowId) const {
    // Need to call const_cast here because ensureLoaded is not marked const
    // but we need to call it from a const method
    const_cast<RocksDBStorage*>(this)->ensureLoaded();
    
    auto res = std::upper_bound(tableData.begin(), tableData.end(), rowId, 
        [](size_t rowId, const TableChunk& chunk) { 
            return rowId < chunk.startRowId + chunk.numRows; 
        });
    
    if (res == tableData.end()) {
        throw std::runtime_error("row id out of bounds");
    }
    
    auto& chunk = *res;
    return {&chunk, rowId - chunk.startRowId};
}

size_t RocksDBStorage::getColIndex(const std::string& colName) {
    return schema->GetFieldIndex(colName);
}

void RocksDBStorage::loadTableData() {
    tableData.clear();
    
    // Iterate through all keys with prefix "batch_"
    std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(rocksdb::ReadOptions()));
    std::string prefix = "batch_";
    
    for (it->Seek(prefix); it->Valid() && it->key().ToString().starts_with(prefix); it->Next()) {
        std::string key = it->key().ToString();
        std::string value = it->value().ToString();
        
        // Extract the row ID from the key
        size_t startRowId = std::stoull(key.substr(prefix.length()));
        
        // Deserialize the batch
        auto buffer = std::make_shared<arrow::Buffer>(value);
        auto reader = std::make_shared<arrow::io::BufferReader>(buffer);
        
        arrow::ipc::IpcReadOptions options = arrow::ipc::IpcReadOptions::Defaults();
        auto result = arrow::ipc::ReadRecordBatch(schema, nullptr, options, reader.get());
        
        if (!result.ok()) {
            throw std::runtime_error("Failed to deserialize batch: " + result.status().ToString());
        }
        
        std::shared_ptr<arrow::RecordBatch> batch = result.ValueOrDie();
        tableData.push_back(TableChunk{batch, startRowId});
    }
    
    // Sort by startRowId to ensure correct ordering
    std::sort(tableData.begin(), tableData.end(), 
        [](const TableChunk& a, const TableChunk& b) { 
            return a.startRowId < b.startRowId; 
        });
}

void RocksDBStorage::loadSchemaAndStats() {
    // Load schema
    std::string schema_val;
    if (db->Get(rocksdb::ReadOptions(), "__schema__", &schema_val).ok()) {
        // Deserialize Arrow schema
        auto buffer = std::make_shared<arrow::Buffer>(schema_val);
        auto reader = std::make_shared<arrow::io::BufferReader>(buffer);
        
        arrow::ipc::DictionaryMemo dictMemo;
        auto result = arrow::ipc::ReadSchema(reader.get(), &dictMemo);
        
        if (result.ok()) {
            schema = result.ValueOrDie();
        }
    }
    
    // Load statistics
    std::string stats_val;
    if (db->Get(rocksdb::ReadOptions(), "__stats__", &stats_val).ok()) {
        // TODO: Deserialize stats from binary format
        // For now, we'll just rely on the in-memory stats
    }
    
    // Load row count
    std::string count_val;
    if (db->Get(rocksdb::ReadOptions(), "__row_count__", &count_val).ok()) {
        numRows = std::stoull(count_val);
    }
}

void RocksDBStorage::storeSchemaAndStats() {
    rocksdb::WriteBatch batch;
    
    // Store schema
    std::shared_ptr<arrow::Buffer> schema_buffer;
    auto result = arrow::ipc::SerializeSchema(*schema);
    if (result.ok()) {
        schema_buffer = result.ValueOrDie();
        batch.Put("__schema__", schema_buffer->ToString());
    }
    
    // Store row count
    batch.Put("__row_count__", std::to_string(numRows));
    
    // Store statistics (simplified for now)
    // TODO: Implement proper serialization of column statistics
    
    db->Write(rocksdb::WriteOptions(), &batch);
}

std::shared_ptr<arrow::DataType> RocksDBStorage::toPhysicalType(lingodb::catalog::Type t) {
    using namespace lingodb::catalog;
    
    // Use getTypeId() instead of getType()
    switch (t.getTypeId()) {
        case LogicalTypeId::BOOLEAN:
            return arrow::boolean();
        case LogicalTypeId::INT:
            // Handle different integer widths
            if (auto intInfo = t.getInfo<IntTypeInfo>()) {
                switch (intInfo->getBitWidth()) {
                    case 8: return arrow::int8();
                    case 16: return arrow::int16();
                    case 32: return arrow::int32();
                    case 64: return arrow::int64();
                    default: throw std::runtime_error("Unsupported integer bit width");
                }
            }
            return arrow::int32(); // Default to int32 if no specific info
        case LogicalTypeId::FLOAT:
            return arrow::float32();
        case LogicalTypeId::DOUBLE:
            return arrow::float64();
        case LogicalTypeId::DECIMAL:
            if (auto decimalInfo = t.getInfo<DecimalTypeInfo>()) {
                return arrow::decimal128(decimalInfo->getPrecision(), decimalInfo->getScale());
            }
            return arrow::decimal128(10, 2); // Default precision and scale
        case LogicalTypeId::DATE:
            if (auto dateInfo = t.getInfo<DateTypeInfo>()) {
                if (dateInfo->getUnit() == DateTypeInfo::DateUnit::DAY) {
                    return arrow::date32();
                } else {
                    return arrow::date64();
                }
            }
            return arrow::date32(); // Default to date32
        case LogicalTypeId::TIMESTAMP:
            return arrow::timestamp(arrow::TimeUnit::NANO);
        case LogicalTypeId::INTERVAL:
            return arrow::duration(arrow::TimeUnit::NANO);
        case LogicalTypeId::CHAR:
        case LogicalTypeId::STRING:
            return arrow::utf8();
        default:
            throw std::runtime_error("Type not supported");
    }
}

std::optional<size_t> RocksDBStorage::countDistinctValues(std::shared_ptr<arrow::ChunkedArray> column) {
    // Implementation copied from LingoDBTable
    if (column->null_count() == column->length()) {
        return 1; // only NULL
    }
    
    // Use arrow::compute::CallFunction instead of Count
    auto result = arrow::compute::CallFunction("count_distinct", {column});
    
    if (!result.ok()) {
        return std::nullopt;
    }
    
    auto count = result.ValueOrDie().scalar_as<arrow::Int64Scalar>().value;
    return count;
}

std::unique_ptr<scheduler::Task> RocksDBStorage::createScanTask(const ScanConfig& scanConfig) {
    ensureLoaded();
    std::vector<size_t> colIds;
    for (const auto& c : scanConfig.columns) {
        auto colId = schema->GetFieldIndex(c);
        assert(colId >= 0);
        colIds.push_back(colId);
    }
    
    // Convert from TableStorage::ScanConfig to RocksDBStorage::ScanConfig
    if (scanConfig.parallel) {
        return std::make_unique<ScanBatchesTask>(tableData, colIds, scanConfig.cb);
    } else {
        return std::make_unique<ScanBatchesSingleThreadedTask>(tableData, colIds, scanConfig.cb);
    }
}

} // namespace lingodb::runtime
