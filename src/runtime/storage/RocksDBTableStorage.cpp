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
         // char(1) is stored as fixed_size_binary(4) for UTF-8 compatibility
         // other char types are stored as strings
         size_t length = charInfo->getLength();
         if (length == 1) {
            return arrow::fixed_size_binary(4);
         }
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

std::shared_ptr<arrow::RecordBatch> createSample(const std::vector<lingodb::runtime::RocksDBTableStorage::TableChunk>& data) {
    size_t numRows = 0;
    for (auto& batch : data) {
        numRows += batch.data()->num_rows();
    }
    if (numRows == 0) {
        return std::shared_ptr<arrow::RecordBatch>();
    }
    
    std::vector<size_t> result;
    auto rng = std::mt19937{std::random_device{}()};
    
    // Simple random sampling implementation
    size_t sampleSize = std::min<size_t>(numRows, 1024ull);
    for (size_t i = 0; i < sampleSize; ++i) {
        result.push_back(rng() % numRows);
    }
    std::sort(result.begin(), result.end());
    
    // For simplicity, just return the first batch as sample
    // In a full implementation, we'd sample across all batches
    if (!data.empty()) {
        return data[0].data();
    }
    return nullptr;
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

// RocksDB-specific scan task implementation
class RocksDBScanTask : public scheduler::TaskWithImplicitContext {
    std::vector<RocksDBTableStorage::TableChunk>& batches;
    std::vector<size_t> colIds;
    std::function<void(lingodb::runtime::BatchView*)> cb;

public:
    RocksDBScanTask(std::vector<RocksDBTableStorage::TableChunk>& batches, 
                   std::vector<size_t> colIds, 
                   const std::function<void(lingodb::runtime::BatchView*)>& cb) 
        : batches(batches), colIds(colIds), cb(cb) {}

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

        for (auto& batch : batches) {
            utility::Tracer::Trace trace(processMorselSingle);
            batchView.length = batch.getNumRows();
            for (size_t i = 0; i < colIds.size(); i++) {
                batchView.arrays[i] = batch.getArrayView(colIds[i]);
            }
            cb(&batchView);
            trace.stop();
        }
    }
    
    ~RocksDBScanTask() = default;
};

// TableChunk implementation
RocksDBTableStorage::TableChunk::TableChunk(std::shared_ptr<arrow::RecordBatch> data, size_t startRowId) 
    : internalData(data), startRowId(startRowId), numRows(data->num_rows()) {
    std::vector<size_t> bufferStart;
    for (auto colId = 0; colId < data->num_columns(); colId++) {
        auto arrayData = data->column(colId)->data();
        size_t currBufId = buffers.size();
        for (size_t i = 0; i < arrayData->buffers.size(); i++) {
            auto buffer = arrayData->buffers[i];
            if (buffer) {
                buffers.push_back(buffer->data());
            } else {
                buffers.push_back(nullptr);
            }
        }
        if (!buffers[currBufId]) {
            buffers[currBufId] = ArrayView::validData.data();
        }
        bufferStart.push_back(currBufId);
    }
    for (auto colId = 0; colId < data->num_columns(); colId++) {
        auto arrayData = data->column(colId)->data();
        columnInfo.push_back(ArrayView{
            .length = arrayData->length,
            .nullCount = arrayData->null_count,
            .offset = arrayData->offset,
            .nBuffers = static_cast<int64_t>(arrayData->buffers.size()),
            .nChildren = static_cast<int64_t>(arrayData->child_data.size()),
            .buffers = &buffers[bufferStart.at(colId)],
            .children = nullptr
        });
    }
}

// RocksDBTableStorage implementation
RocksDBTableStorage::RocksDBTableStorage(std::shared_ptr<RocksDBStorage> storage,
                                       const std::string& tableName,
                                       std::shared_ptr<arrow::Schema> schema)
    : storage(storage), tableName(tableName), schema(schema), sample(schema), numRows(0) {
    for (auto c : schema->fields()) {
        columnStatistics[c->name()] = catalog::ColumnStatistics(std::nullopt);
    }
}

RocksDBTableStorage::RocksDBTableStorage(std::shared_ptr<RocksDBStorage> storage,
                                       const std::string& tableName,
                                       std::shared_ptr<arrow::Schema> schema,
                                       size_t numRows,
                                       catalog::Sample sample,
                                       ColumnStatisticsMap columnStatistics)
    : storage(storage), tableName(tableName), schema(schema), sample(std::move(sample)),
      numRows(numRows), columnStatistics(std::move(columnStatistics)) {}

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
    ensureLoaded();
    
    size_t currentChunkId = getChunkCount();
    
    for (auto& batch : toAppend) {
        if (batch->schema()->Equals(*schema)) {
            // Store the chunk in RocksDB
            storeChunk(currentChunkId, batch);
            
            // Update in-memory cache
            chunkCache.push_back(TableChunk{batch, numRows});
            numRows += batch->num_rows();
            currentChunkId++;
        } else {
            std::cout << "schema to add: " << batch->schema()->ToString() << std::endl;
            std::cout << "schema of table: " << schema->ToString() << std::endl;
            throw std::runtime_error("schema mismatch");
        }
    }
    
    // Update statistics (only if we have data)
    if (!toAppend.empty()) {
        auto tableView = arrow::Table::FromRecordBatches(toAppend).ValueOrDie();
        updateStatistics(tableView);
    }
    
    // Update sample
    auto sampleBatch = createSample(chunkCache);
    if (sampleBatch) {
        sample = catalog::Sample(sampleBatch);
    }
    
    // Flush metadata
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

void RocksDBTableStorage::ensureLoaded() const {
    if (!loaded) {
        const_cast<RocksDBTableStorage*>(this)->loaded = true;
        const_cast<RocksDBTableStorage*>(this)->deserializeMetadata();
        if (numRows > 0) {
            const_cast<RocksDBTableStorage*>(this)->loadAllChunks();
        }
    }
}

std::pair<const RocksDBTableStorage::TableChunk*, size_t> RocksDBTableStorage::getByRowId(size_t rowId) const {
    ensureLoaded();
    
    size_t currentRow = 0;
    for (const auto& chunk : chunkCache) {
        if (rowId >= currentRow && rowId < currentRow + chunk.getNumRows()) {
            return {&chunk, rowId - currentRow};
        }
        currentRow += chunk.getNumRows();
    }
    
    // Return nullptr for invalid row IDs instead of throwing an exception
    return {nullptr, 0};
}

// Key generation methods
std::string RocksDBTableStorage::getChunkKey(size_t chunkId) const {
    return "chunk:" + tableName + ":" + std::to_string(chunkId);
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

// Arrow serialization helpers
std::string RocksDBTableStorage::serializeRecordBatch(const std::shared_ptr<arrow::RecordBatch>& batch) const {
    arrow::ipc::IpcWriteOptions options;
    auto buffer = arrow::ipc::SerializeRecordBatch(*batch, options).ValueOrDie();
    return std::string(reinterpret_cast<const char*>(buffer->data()), buffer->size());
}

std::shared_ptr<arrow::RecordBatch> RocksDBTableStorage::deserializeRecordBatch(const std::string& data) const {
    auto buffer = std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t*>(data.data()), data.size());
    arrow::ipc::DictionaryMemo dict_memo;
    arrow::ipc::IpcReadOptions options;
    auto bufferReader = arrow::io::BufferReader::FromString(data);
    auto result = arrow::ipc::ReadRecordBatch(schema, &dict_memo, options, bufferReader.get());
    if (!result.ok()) {
        throw std::runtime_error("Failed to deserialize record batch: " + result.status().ToString());
    }
    return result.ValueOrDie();
}

// Chunk management
void RocksDBTableStorage::storeChunk(size_t chunkId, const std::shared_ptr<arrow::RecordBatch>& batch) {
    std::string key = getChunkKey(chunkId);
    std::string value = serializeRecordBatch(batch);
    
    auto status = storage->put(RocksDBStorage::ColumnFamily::TABLES, key, value);
    if (!status.ok()) {
        throw std::runtime_error("Failed to store chunk: " + status.ToString());
    }
}

std::shared_ptr<arrow::RecordBatch> RocksDBTableStorage::loadChunk(size_t chunkId) const {
    std::string key = getChunkKey(chunkId);
    std::string value;
    
    auto status = storage->get(RocksDBStorage::ColumnFamily::TABLES, key, &value);
    if (!status.ok()) {
        throw std::runtime_error("Failed to load chunk: " + status.ToString());
    }
    
    return deserializeRecordBatch(value);
}

void RocksDBTableStorage::updateStatistics(const std::shared_ptr<arrow::Table>& table) {
    for (auto c : schema->fields()) {
        auto column = table->GetColumnByName(c->name());
        if (column) {
            columnStatistics[c->name()] = catalog::ColumnStatistics(countDistinctValues(column));
        }
    }
}

void RocksDBTableStorage::invalidateCache() const {
    cacheValid = false;
    chunkCache.clear();
}

void RocksDBTableStorage::loadAllChunks() {
    if (cacheValid) return;
    
    chunkCache.clear();
    auto chunkIds = getChunkIds();
    
    size_t currentRowId = 0;
    for (size_t chunkId : chunkIds) {
        auto batch = loadChunk(chunkId);
        chunkCache.push_back(TableChunk{batch, currentRowId});
        currentRowId += batch->num_rows();
    }
    
    cacheValid = true;
}

size_t RocksDBTableStorage::getChunkCount() const {
    // Count chunks by iterating through keys with chunk prefix
    auto iterator = storage->newIterator(RocksDBStorage::ColumnFamily::TABLES);
    std::string prefix = "chunk:" + tableName + ":";
    
    size_t count = 0;
    iterator->Seek(prefix);
    while (iterator->Valid()) {
        std::string key = iterator->key().ToString();
        if (key.substr(0, prefix.length()) != prefix) {
            break;
        }
        count++;
        iterator->Next();
    }
    
    return count;
}

std::vector<size_t> RocksDBTableStorage::getChunkIds() const {
    auto iterator = storage->newIterator(RocksDBStorage::ColumnFamily::TABLES);
    std::string prefix = "chunk:" + tableName + ":";
    
    std::vector<size_t> chunkIds;
    iterator->Seek(prefix);
    while (iterator->Valid()) {
        std::string key = iterator->key().ToString();
        if (key.substr(0, prefix.length()) != prefix) {
            break;
        }
        
        // Extract chunk ID from key
        std::string chunkIdStr = key.substr(prefix.length());
        size_t chunkId = std::stoull(chunkIdStr);
        chunkIds.push_back(chunkId);
        
        iterator->Next();
    }
    
    std::sort(chunkIds.begin(), chunkIds.end());
    return chunkIds;
}

void RocksDBTableStorage::serializeMetadata() const {
    // Serialize basic metadata
    std::ostringstream metadataStream;
    metadataStream << numRows << "|" << schema->num_fields();
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
        std::string sampleValue = serializeRecordBatch(sample.getSampleData());
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
        auto sampleBatch = deserializeRecordBatch(sampleValue);
        if (sampleBatch) {
            const_cast<RocksDBTableStorage*>(this)->sample = catalog::Sample(sampleBatch);
        }
    }
}

std::unique_ptr<scheduler::Task> RocksDBTableStorage::createScanTask(const ScanConfig& scanConfig) {
    ensureLoaded();
    std::vector<size_t> colIds;
    for (const auto& c : scanConfig.columns) {
        auto colId = schema->GetFieldIndex(c);
        assert(colId >= 0);
        colIds.push_back(colId);
    }
    
    // For simplicity, always use single-threaded for now
    // Could implement parallel scanning similar to LingoDBTable
    return std::make_unique<RocksDBScanTask>(chunkCache, colIds, scanConfig.cb);
}

} // namespace lingodb::runtime

#endif // WITH_ROCKSDB 