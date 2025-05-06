#pragma once

#include <rocksdb/db.h>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <arrow/api.h>
#include "lingodb/catalog/Defs.h"
#include "lingodb/catalog/MetaData.h"
#include "lingodb/runtime/RecordBatchInfo.h"
#include "lingodb/runtime/storage/TableStorage.h"
#include "lingodb/scheduler/Tasks.h"

namespace lingodb::runtime {

// Forward declaration
class BatchesWorkerResvState;

class RocksDBStorage {
public:
    struct TableChunk : public runtime::TableChunk {
        std::shared_ptr<arrow::RecordBatch> data() const override { return internalData; }
        size_t getNumRows() const override { return numRows; }
        
        std::shared_ptr<arrow::RecordBatch> internalData;
        size_t startRowId;
        size_t numRows;
        std::vector<ColumnInfo> columnInfo;
        TableChunk(std::shared_ptr<arrow::RecordBatch> data, size_t startRowId);
    };

    static std::unique_ptr<RocksDBStorage> create(const catalog::CreateTableDef& def);
    RocksDBStorage(const std::string& db_path, std::shared_ptr<arrow::Schema> schema);
    RocksDBStorage(const std::string& db_path, std::shared_ptr<arrow::Schema> schema, 
                size_t numRows, const catalog::Sample& sample,
                const std::unordered_map<std::string, catalog::ColumnStatistics>& columnStats)
        : db_path(db_path), schema(schema), sample(sample), columnStatistics(columnStats), numRows(numRows), persist(true) {
        rocksdb::Options options;
        options.create_if_missing = true;
        rocksdb::DB* db_ptr = nullptr;
        auto status = rocksdb::DB::Open(options, db_path, &db_ptr);
        if (!status.ok()) {
            throw std::runtime_error("Failed to open RocksDB: " + status.ToString());
        }
        db.reset(db_ptr);
    }

    void append(const std::shared_ptr<arrow::Table>& table);
    void append(const std::vector<std::shared_ptr<arrow::RecordBatch>>& toAppend);
    void flush();
    const catalog::ColumnStatistics& getColumnStatistics(const std::string& column) const;
    void ensureLoaded();
    std::shared_ptr<arrow::Schema> getSchema() const { return schema; }
    size_t getNumRows() const { return numRows; }
    const catalog::Sample& getSample() const { return sample; }

    // Create a scan task for processing the table data
    std::unique_ptr<scheduler::Task> createScanTask(const ScanConfig& scanConfig);

    // Get a specific row by its ID
    std::pair<const TableChunk*, size_t> getByRowId(size_t rowId) const;
    size_t getColIndex(const std::string& colName);

    // Static helper functions
    static std::shared_ptr<arrow::DataType> toPhysicalType(lingodb::catalog::Type t);
    static std::optional<size_t> countDistinctValues(std::shared_ptr<arrow::ChunkedArray> column);

private:
    std::unique_ptr<rocksdb::DB> db;
    std::string db_path;
    std::shared_ptr<arrow::Schema> schema;
    catalog::Sample sample;
    std::unordered_map<std::string, catalog::ColumnStatistics> columnStatistics;
    std::vector<TableChunk> tableData;
    size_t numRows;
    bool persist;

    void loadTableData();
    void loadSchemaAndStats();
    void storeSchemaAndStats();
};

// Task classes for scanning batches (similar to LingoDBTable)
class ScanBatchesTask : public scheduler::TaskWithImplicitContext {
    std::vector<RocksDBStorage::TableChunk> batches;
    std::vector<size_t> colIds;
    std::function<void(RecordBatchInfo*)> cb;
    // std::vector<std::unique_ptr<BatchesWorkerResvState>> workerResvs; // Disabled due to missing definition
    std::vector<RecordBatchInfo*> batchInfos;

public:
    ScanBatchesTask(std::vector<RocksDBStorage::TableChunk> batches, std::vector<size_t> colIds, 
                   const std::function<void(RecordBatchInfo*)>& cb)
        : batches(std::move(batches)), colIds(std::move(colIds)), cb(cb) {
        // Implementation would be similar to LingoDBTable's ScanBatchesTask
    }

    bool allocateWork() override { 
        // Implementation would be similar to LingoDBTable's ScanBatchesTask
        return false; 
    }
    
    void performWork() override {
        // Implementation would be similar to LingoDBTable's ScanBatchesTask
    }
    
    ~ScanBatchesTask() {
        // Cleanup
    }
};

class ScanBatchesSingleThreadedTask : public scheduler::TaskWithImplicitContext {
    std::vector<RocksDBStorage::TableChunk> batches;
    std::vector<size_t> colIds;
    std::function<void(RecordBatchInfo*)> cb;

public:
    ScanBatchesSingleThreadedTask(std::vector<RocksDBStorage::TableChunk> batches, std::vector<size_t> colIds, 
                                 const std::function<void(RecordBatchInfo*)>& cb)
        : batches(std::move(batches)), colIds(std::move(colIds)), cb(cb) {
    }

    bool allocateWork() override { 
        // Implementation would be similar to LingoDBTable's ScanBatchesSingleThreadedTask
        return false; 
    }
    
    void performWork() override {
        // Implementation would be similar to LingoDBTable's ScanBatchesSingleThreadedTask
    }
    
    ~ScanBatchesSingleThreadedTask() {
        // Cleanup
    }
};

} // namespace lingodb::runtime
