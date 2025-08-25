#ifdef WITH_ROCKSDB

#include "lingodb/runtime/storage/RocksDBTableStorage.h"
#include "lingodb/catalog/Defs.h"
#include "lingodb/catalog/Types.h"
#include "lingodb/runtime/ArrowView.h"
#include "lingodb/runtime/DataSourceIteration.h"
#include "lingodb/scheduler/Task.h"
#include "lingodb/scheduler/Tasks.h"
#include "lingodb/utility/Serialization.h"
#include "lingodb/utility/Tracer.h"

#include <arrow/builder.h>
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/ipc/writer.h>
#include <arrow/ipc/reader.h>
#include <arrow/table.h>

#include <algorithm>
#include <iostream>
#include <random>
#include <ranges>
#include <sstream>
#include <cassert>

namespace {

// Helper function to convert catalog types to Arrow types
std::shared_ptr<arrow::DataType> toPhysicalType(lingodb::catalog::Type t) {
   using TypeId = lingodb::catalog::LogicalTypeId;
   switch (t.getTypeId()) {
      case TypeId::BOOLEAN:
         return arrow::boolean();
      case TypeId::INT:
         switch (t.getInfo<lingodb::catalog::IntTypeInfo>()->getBitWidth()) {
            case 8:
               return arrow::int8();
            case 16:
               return arrow::int16();
            case 32:
               return arrow::int32();
            case 64:
               return arrow::int64();
            default:
               throw std::runtime_error("unsupported bit width");
         }
      case TypeId::FLOAT:
         return arrow::float32();
      case TypeId::DOUBLE:
         return arrow::float64();
      case TypeId::DECIMAL:
         return arrow::decimal128(t.getInfo<lingodb::catalog::DecimalTypeInfo>()->getPrecision(),
                                  t.getInfo<lingodb::catalog::DecimalTypeInfo>()->getScale());
      case TypeId::DATE: {
         auto dateUnit = t.getInfo<lingodb::catalog::DateTypeInfo>()->getUnit();
         switch (dateUnit) {
            case lingodb::catalog::DateTypeInfo::DateUnit::DAY:
               return arrow::date32();
            case lingodb::catalog::DateTypeInfo::DateUnit::MILLIS:
               return arrow::date64();
            default:
               return arrow::date32(); // Default to date32
         }
         break;
      }
      case TypeId::TIMESTAMP: {
         arrow::TimeUnit::type timeUnit;
         auto logicalTimeUnit = t.getInfo<lingodb::catalog::TimestampTypeInfo>()->getUnit();
         switch (logicalTimeUnit) {
            case lingodb::catalog::TimestampTypeInfo::TimestampUnit::NANOS:
               timeUnit = arrow::TimeUnit::NANO;
               break;
            case lingodb::catalog::TimestampTypeInfo::TimestampUnit::MICROS:
               timeUnit = arrow::TimeUnit::MICRO;
               break;
            case lingodb::catalog::TimestampTypeInfo::TimestampUnit::MILLIS:
               timeUnit = arrow::TimeUnit::MILLI;
               break;
            case lingodb::catalog::TimestampTypeInfo::TimestampUnit::SECONDS:
               timeUnit = arrow::TimeUnit::SECOND;
               break;
            default:
               timeUnit = arrow::TimeUnit::MICRO; // Default to micro
               break;
         }
         return arrow::timestamp(timeUnit);
         break;
      }
      case TypeId::INTERVAL: {
         auto intervalUnit = t.getInfo<lingodb::catalog::IntervalTypeInfo>()->getUnit();
         switch (intervalUnit) {
            case lingodb::catalog::IntervalTypeInfo::IntervalUnit::MONTH:
               return arrow::duration(arrow::TimeUnit::MICRO);
            case lingodb::catalog::IntervalTypeInfo::IntervalUnit::DAYTIME:
               return arrow::duration(arrow::TimeUnit::MICRO);
            default:
               return arrow::duration(arrow::TimeUnit::MICRO); // Default to micro
         }
         break;
      }
      case TypeId::CHAR: {
         auto charInfo = t.getInfo<lingodb::catalog::CharTypeInfo>();
         size_t length = charInfo->getLength();
         // Store all CHAR types as UTF-8 strings for consistency
         // This avoids issues with fixed_size_binary conversion
         // and ensures proper UTF-8 handling
         return arrow::utf8();
      }
         break;
      case TypeId::STRING:
         return arrow::utf8();
         break;
      default:
         throw std::runtime_error("unsupported type");
   }
}

namespace utility = lingodb::utility;
// Commented out unused variable to fix warning
// static utility::Tracer::Event processMorsel("DataSourceIteration", "processMorsel");
static utility::Tracer::Event processMorselSingle("DataSourceIteration", "processMorselSingle");

// Helper function to create sample data from a record batch
std::shared_ptr<arrow::RecordBatch> createSample(const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (!batch || batch->num_rows() == 0) {
        return nullptr;
    }
    
    // For simplicity, if the batch is small enough, use it as-is
    if (batch->num_rows() <= 1024) {
        return batch;
    }
    
    // Otherwise, take the first 1024 rows as sample
    return batch->Slice(0, 1024);
}

size_t countDistinctValues(std::shared_ptr<arrow::ChunkedArray> column) {
    // Simple distinct count implementation
    // In practice, you'd use HyperLogLog or similar for large datasets
    if (!column || column->num_chunks() == 0) {
        return 0;
    }
    
    // For simplicity, return estimated distinct count based on length
    // A real implementation would use proper cardinality estimation
    size_t totalLength = column->length();
    return std::min(static_cast<size_t>(totalLength), static_cast<size_t>(totalLength * 0.8));
}

} // namespace

namespace lingodb::runtime {

// RocksDB-specific direct scan task implementation
class RocksDBDirectScanTask : public scheduler::TaskWithImplicitContext {
    RocksDBTableStorage* storage;
    std::vector<size_t> colIds;
    std::function<void(lingodb::runtime::BatchView*)> cb;
    size_t totalChunks;

public:
    RocksDBDirectScanTask(RocksDBTableStorage* storage,
                         const std::vector<size_t>& colIds,
                         const std::function<void(lingodb::runtime::BatchView*)>& cb,
                         size_t totalChunks)
        : storage(storage), colIds(colIds), cb(cb), totalChunks(totalChunks) {}

    bool allocateWork() override {
        if (!workExhausted.exchange(true)) {
            return true;
        }
        return false;
    }
    
    void performWork() override {
        BatchView batchView;
        std::vector<const ArrayView*> arrayViewPtrs(colIds.size());
        batchView.arrays = arrayViewPtrs.data();
        batchView.offset = 0;
        batchView.length = 0;
        batchView.selectionVector = nullptr;
        
        // Process all chunks in one go, similar to original implementation
        for (size_t chunkId = 0; chunkId < totalChunks; chunkId++) {
            auto batch = storage->loadRecordBatch(chunkId);
            if (!batch || batch->num_rows() == 0) continue;
            
            // Convert to ArrayView format for processing
            std::vector<ArrayView> columnViews;
            std::vector<std::vector<const void*>> bufferStorage;
            columnViews.reserve(colIds.size());
            bufferStorage.reserve(colIds.size());
            
            for (size_t i = 0; i < colIds.size(); i++) {
                auto array = batch->column(colIds[i]);
                ArrayView view;
                view.length = array->length();
                view.nullCount = array->null_count();
                view.offset = array->offset();
                
                // Get buffers from Arrow array
                std::vector<const void*> buffers;
                auto arrayData = array->data();
                for (const auto& buffer : arrayData->buffers) {
                    if (buffer) {
                        buffers.push_back(buffer->data());
                    } else {
                        buffers.push_back(nullptr);
                    }
                }
                if (!buffers.empty() && !buffers[0]) {
                    buffers[0] = ArrayView::validData.data();
                }
                bufferStorage.push_back(buffers);
                
                view.nBuffers = buffers.size();
                view.buffers = bufferStorage.back().data();
                view.nChildren = 0;
                view.children = nullptr;
                
                columnViews.push_back(view);
            }
            
            // Update BatchView with this chunk's data
            std::vector<const ArrayView*> chunkArrayViewPtrs;
            for (auto& view : columnViews) {
                chunkArrayViewPtrs.push_back(&view);
            }
            
            batchView.arrays = chunkArrayViewPtrs.data();
            batchView.length = batch->num_rows();
            
            utility::Tracer::Trace trace(processMorselSingle);
            cb(&batchView);
            trace.stop();
        }
    }
    
    ~RocksDBDirectScanTask() = default;
};

// Column-oriented storage implementation
void RocksDBTableStorage::storeColumn(const std::string& columnName, size_t chunkId, 
                                      const std::shared_ptr<arrow::Array>& array) {
    std::string key = getColumnKey(columnName, chunkId);
    
    // Get the field from schema to ensure consistent type storage
    auto field = schema->GetFieldByName(columnName);
    if (!field) {
        throw std::runtime_error("Column not found in schema: " + columnName);
    }
    
    // If types don't match exactly, cast the array to the schema's type
    std::shared_ptr<arrow::Array> arrayToStore = array;
    if (!array->type()->Equals(field->type())) {
        // Cast to the expected type
        arrow::compute::CastOptions cast_options;
        cast_options.allow_invalid_utf8 = true;
        auto result = arrow::compute::Cast(*array, field->type(), cast_options);
        if (!result.ok()) {
            throw std::runtime_error("Failed to cast array to schema type: " + result.status().ToString());
        }
        // Cast returns a Result<Datum> which contains the array
        arrow::Datum datum = result.ValueOrDie();
        arrayToStore = datum.make_array();
    }
    
    auto tempSchema = arrow::schema({field});
    auto batch = arrow::RecordBatch::Make(tempSchema, arrayToStore->length(), {arrayToStore});
    
    // Serialize the batch
    arrow::ipc::IpcWriteOptions options;
    auto result = arrow::ipc::SerializeRecordBatch(*batch, options);
    if (!result.ok()) {
        throw std::runtime_error("Failed to serialize column: " + result.status().ToString());
    }
    
    auto buffer = result.ValueOrDie();
    std::string value(reinterpret_cast<const char*>(buffer->data()), buffer->size());
    
    auto status = storage->put(RocksDBStorage::ColumnFamily::TABLES, key, value);
    if (!status.ok()) {
        throw std::runtime_error("Failed to store column: " + status.ToString());
    }
}

std::shared_ptr<arrow::Array> RocksDBTableStorage::loadColumn(const std::string& columnName, 
                                                              size_t chunkId) const {
    std::string key = getColumnKey(columnName, chunkId);
    std::string value;
    
    auto status = storage->get(RocksDBStorage::ColumnFamily::TABLES, key, &value);
    if (!status.ok()) {
        if (status.IsNotFound()) {
            return nullptr;
        }
        throw std::runtime_error("Failed to load column: " + status.ToString());
    }
    
    // Get the field for this column
    auto field = schema->GetFieldByName(columnName);
    if (!field) {
        throw std::runtime_error("Column not found in schema: " + columnName);
    }
    
    // Create a temporary schema for deserialization
    auto tempSchema = arrow::schema({field});
    
    // Deserialize the record batch
    arrow::ipc::DictionaryMemo dict_memo;
    arrow::ipc::IpcReadOptions read_options;
    auto bufferReader = arrow::io::BufferReader::FromString(value);
    
    auto result = arrow::ipc::ReadRecordBatch(tempSchema, &dict_memo, read_options, bufferReader.get());
    if (!result.ok()) {
        std::cerr << "Failed to deserialize column " << columnName << " for chunk " << chunkId << std::endl;
        std::cerr << "Data size: " << value.size() << " bytes" << std::endl;
        throw std::runtime_error("Failed to deserialize column: " + result.status().ToString());
    }
    
    // Extract the array from the batch
    auto batch = result.ValueOrDie();
    if (batch && batch->num_columns() > 0) {
        return batch->column(0);
    }
    
    return nullptr;
}

RocksDBTableStorage::ColumnBuffer RocksDBTableStorage::loadColumnBuffer(const std::string& columnName, 
                                                                        size_t chunkId) const {
    ColumnBuffer buffer;
    buffer.chunkId = chunkId;
    
    std::string key = getColumnKey(columnName, chunkId);
    auto status = storage->get(RocksDBStorage::ColumnFamily::TABLES, key, &buffer.data);
    
    if (!status.ok()) {
        throw std::runtime_error("Failed to load column buffer: " + status.ToString());
    }
    
    // For now, we'll deserialize to get the row count
    // In a production system, this would be stored separately
    auto array = loadColumn(columnName, chunkId);
    if (array) {
        buffer.numRows = array->length();
    }
    
    return buffer;
}

// RocksDBTableStorage implementation
RocksDBTableStorage::RocksDBTableStorage(std::shared_ptr<RocksDBStorage> storage,
                                       const std::string& tableName,
                                       std::shared_ptr<arrow::Schema> schema)
    : storage(storage), tableName(tableName), schema(schema), sample(schema), numRows(0), numChunks(0) {
    for (auto c : schema->fields()) {
        columnStatistics[c->name()] = catalog::ColumnStatistics(std::nullopt);
    }
    deserializeMetadata();
}

RocksDBTableStorage::RocksDBTableStorage(std::shared_ptr<RocksDBStorage> storage,
                                       const std::string& tableName,
                                       std::shared_ptr<arrow::Schema> schema,
                                       size_t numRows,
                                       catalog::Sample sample,
                                       ColumnStatisticsMap columnStatistics)
    : storage(storage), tableName(tableName), schema(schema), sample(std::move(sample)),
      numRows(numRows), numChunks((numRows + CHUNK_SIZE - 1) / CHUNK_SIZE),
      columnStatistics(std::move(columnStatistics)) {}

std::unique_ptr<RocksDBTableStorage> RocksDBTableStorage::create(std::shared_ptr<RocksDBStorage> storage,
                                                               const catalog::CreateTableDef& def) {
    arrow::FieldVector fields;
    for (auto c : def.columns) {
        fields.push_back(std::make_shared<arrow::Field>(std::string{c.getColumnName()}, toPhysicalType(c.getLogicalType()), c.getIsNullable()));
    }
    auto arrowSchema = std::make_shared<arrow::Schema>(fields);
    return std::make_unique<RocksDBTableStorage>(storage, def.name, arrowSchema);
}

void RocksDBTableStorage::append(const std::shared_ptr<arrow::Table>& table) {
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

void RocksDBTableStorage::append(const std::vector<std::shared_ptr<arrow::RecordBatch>>& toAppend) {
    size_t currentChunkId = numChunks;
    
    for (auto& batch : toAppend) {
        // Check schema compatibility (ignoring nullability differences)
        if (batch->schema()->num_fields() != schema->num_fields()) {
            std::cout << "schema to add: " << batch->schema()->ToString() << std::endl;
            std::cout << "schema of table: " << schema->ToString() << std::endl;
            throw std::runtime_error("schema mismatch: different number of fields");
        }
        
        for (int i = 0; i < schema->num_fields(); i++) {
            auto batchField = batch->schema()->field(i);
            auto schemaField = schema->field(i);
            
            // Check field name
            if (batchField->name() != schemaField->name()) {
                std::cout << "schema to add: " << batch->schema()->ToString() << std::endl;
                std::cout << "schema of table: " << schema->ToString() << std::endl;
                throw std::runtime_error("schema mismatch: field name mismatch");
            }
            
            // Check field type compatibility
            // Allow some compatible type conversions:
            // - fixed_size_binary to string
            // - string to fixed_size_binary  
            bool typeCompatible = false;
            
            if (batchField->type()->Equals(schemaField->type())) {
                typeCompatible = true;
            } else if ((batchField->type()->id() == arrow::Type::FIXED_SIZE_BINARY && 
                        schemaField->type()->id() == arrow::Type::STRING) ||
                       (batchField->type()->id() == arrow::Type::STRING && 
                        schemaField->type()->id() == arrow::Type::FIXED_SIZE_BINARY)) {
                // Allow conversion between fixed_size_binary and string
                typeCompatible = true;
            }
            
            if (!typeCompatible) {
                std::cout << "Field " << i << " (" << batchField->name() << "):" << std::endl;
                std::cout << "  Batch type: " << batchField->type()->ToString() << " (id=" << batchField->type()->id() << ")" << std::endl;
                std::cout << "  Table type: " << schemaField->type()->ToString() << " (id=" << schemaField->type()->id() << ")" << std::endl;
                std::cout << "schema to add: " << batch->schema()->ToString() << std::endl;
                std::cout << "schema of table: " << schema->ToString() << std::endl;
                throw std::runtime_error("schema mismatch: field type mismatch");
            }
        }
        
        // Store the batch using column-oriented storage
        storeRecordBatch(currentChunkId, batch);
        
        numRows += batch->num_rows();
        currentChunkId++;
    }
    
    numChunks = currentChunkId;
    
    // Update statistics (only if we have data)
    if (!toAppend.empty()) {
        auto tableView = arrow::Table::FromRecordBatches(toAppend).ValueOrDie();
        updateStatistics(tableView);
        
        // Update sample - use first batch for sampling
        if (!toAppend.empty()) {
            sample = catalog::Sample(toAppend[0]);
        }
    }
    
    // Persist metadata
    serializeMetadata();
    flush();
}

const catalog::ColumnStatistics& RocksDBTableStorage::getColumnStatistics(std::string_view column) const {
    if (!columnStatistics.contains(column)) {
        throw std::runtime_error("MetaData: Column not found");
    }
    return columnStatistics.at(std::string{column});
}

void RocksDBTableStorage::flush() {
    if (!storage) return;
    
    // Flush the RocksDB tables column family
    auto status = storage->flush(RocksDBStorage::ColumnFamily::TABLES);
    if (!status.ok()) {
        throw std::runtime_error("Failed to flush table data: " + status.ToString());
    }
    
    // Flush metadata column family
    status = storage->flush(RocksDBStorage::ColumnFamily::METADATA);
    if (!status.ok()) {
        throw std::runtime_error("Failed to flush metadata: " + status.ToString());
    }
}

std::shared_ptr<arrow::DataType> RocksDBTableStorage::getColumnStorageType(std::string_view columnName) const {
    auto field = schema->GetFieldByName(std::string{columnName});
    if (!field) {
        return nullptr;
    }
    return field->type();
}

// Methods removed - no longer using in-memory cache

// Key generation methods
std::string RocksDBTableStorage::getColumnKey(const std::string& columnName, size_t chunkId) const {
    return "column:" + tableName + ":" + columnName + ":" + std::to_string(chunkId);
}

std::string RocksDBTableStorage::getMetadataKey() const {
    return "metadata:" + tableName;
}

std::string RocksDBTableStorage::getStatisticsKey() const {
    return "statistics:" + tableName;
}

std::string RocksDBTableStorage::getSampleKey() const {
    return "sample:" + tableName;
}

// Batch operations  
void RocksDBTableStorage::storeRecordBatch(size_t chunkId, const std::shared_ptr<arrow::RecordBatch>& batch) {
    // Store each column separately
    for (int i = 0; i < batch->num_columns(); i++) {
        auto column = batch->column(i);
        auto columnName = batch->column_name(i);
        storeColumn(columnName, chunkId, column);
    }
}

std::shared_ptr<arrow::RecordBatch> RocksDBTableStorage::loadRecordBatch(size_t chunkId) const {
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    
    // Load each column
    for (const auto& field : schema->fields()) {
        auto array = loadColumn(field->name(), chunkId);
        if (!array) {
            // If any column is missing, return nullptr
            return nullptr;
        }
        arrays.push_back(array);
        fields.push_back(field);
    }
    
    // Create the record batch
    auto result = arrow::RecordBatch::Make(schema, arrays[0]->length(), arrays);
    return result;
}

void RocksDBTableStorage::updateStatistics(const std::shared_ptr<arrow::Table>& table) {
    for (auto c : schema->fields()) {
        auto column = table->GetColumnByName(c->name());
        if (column) {
            columnStatistics[c->name()] = catalog::ColumnStatistics(countDistinctValues(column));
        }
    }
}

// Utility methods
size_t RocksDBTableStorage::getChunkForRow(size_t rowId) const {
    return rowId / CHUNK_SIZE;
}

size_t RocksDBTableStorage::getRowOffsetInChunk(size_t rowId) const {
    return rowId % CHUNK_SIZE;
}

void RocksDBTableStorage::serializeMetadata() const {
    // Serialize basic metadata
    std::ostringstream metadataStream;
    metadataStream << numRows << "|" << numChunks << "|" << schema->num_fields();
    for (const auto& field : schema->fields()) {
        metadataStream << "|" << field->name();
    }
    
    std::string metadataKey = getMetadataKey();
    auto status = storage->put(RocksDBStorage::ColumnFamily::METADATA, metadataKey, metadataStream.str());
    if (!status.ok()) {
        throw std::runtime_error("Failed to store metadata: " + status.ToString());
    }
    
    // Serialize schema
    auto schemaBuffer = arrow::ipc::SerializeSchema(*schema).ValueOrDie();
    std::string schemaValue(reinterpret_cast<const char*>(schemaBuffer->data()), schemaBuffer->size());
    status = storage->put(RocksDBStorage::ColumnFamily::METADATA, "schema:" + tableName, schemaValue);
    if (!status.ok()) {
        throw std::runtime_error("Failed to store schema: " + status.ToString());
    }
    
    // Store sample if available
    if (sample) {
        // Serialize sample using Arrow IPC
        auto sampleBatch = sample.getSampleData();
        arrow::ipc::IpcWriteOptions options;
        auto buffer = arrow::ipc::SerializeRecordBatch(*sampleBatch, options).ValueOrDie();
        std::string sampleValue(reinterpret_cast<const char*>(buffer->data()), buffer->size());
        status = storage->put(RocksDBStorage::ColumnFamily::METADATA, getSampleKey(), sampleValue);
        if (!status.ok()) {
            throw std::runtime_error("Failed to store sample: " + status.ToString());
        }
    }
}

void RocksDBTableStorage::deserializeMetadata() const {
    // Load basic metadata
    std::string metadataKey = getMetadataKey();
    std::string metadataValue;
    auto status = storage->get(RocksDBStorage::ColumnFamily::METADATA, metadataKey, &metadataValue);
    if (status.ok()) {
        std::istringstream metadataStream(metadataValue);
        std::string token;
        std::getline(metadataStream, token, '|');
        const_cast<RocksDBTableStorage*>(this)->numRows = std::stoull(token);
        if (std::getline(metadataStream, token, '|')) {
            const_cast<RocksDBTableStorage*>(this)->numChunks = std::stoull(token);
        }
    }
    
    // Load schema 
    std::string schemaValue;
    status = storage->get(RocksDBStorage::ColumnFamily::METADATA, "schema:" + tableName, &schemaValue);
    if (status.ok()) {
        auto buffer = std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t*>(schemaValue.data()), schemaValue.size());
        auto bufferReader = arrow::io::BufferReader::FromString(schemaValue);
        arrow::ipc::DictionaryMemo dict_memo;
        auto schemaResult = arrow::ipc::ReadSchema(bufferReader.get(), &dict_memo);
        if (schemaResult.ok()) {
            const_cast<RocksDBTableStorage*>(this)->schema = schemaResult.ValueOrDie();
        }
    }
    
    // Load sample
    std::string sampleValue;
    status = storage->get(RocksDBStorage::ColumnFamily::METADATA, getSampleKey(), &sampleValue);
    if (status.ok()) {
        // Deserialize sample using Arrow IPC
        arrow::ipc::DictionaryMemo dict_memo;
        arrow::ipc::IpcReadOptions options;
        auto bufferReader = arrow::io::BufferReader::FromString(sampleValue);
        auto result = arrow::ipc::ReadRecordBatch(schema, &dict_memo, options, bufferReader.get());
        if (result.ok()) {
            auto sampleBatch = result.ValueOrDie();
            if (sampleBatch) {
                const_cast<RocksDBTableStorage*>(this)->sample = catalog::Sample(sampleBatch);
            }
        }
    }
}

std::unique_ptr<scheduler::Task> RocksDBTableStorage::createScanTask(const ScanConfig& scanConfig) {
    std::vector<size_t> colIds;
    
    for (const auto& c : scanConfig.columns) {
        auto colId = schema->GetFieldIndex(c);
        assert(colId >= 0);
        colIds.push_back(colId);
    }
    
    // Use direct scanning from RocksDB
    return std::make_unique<RocksDBDirectScanTask>(this, colIds, scanConfig.cb, numChunks);
}

} // namespace lingodb::runtime

#endif // WITH_ROCKSDB 