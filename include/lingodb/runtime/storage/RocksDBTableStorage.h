#ifndef LINGODB_RUNTIME_STORAGE_ROCKSDBTABLESTORAGE_H
#define LINGODB_RUNTIME_STORAGE_ROCKSDBTABLESTORAGE_H

#ifdef WITH_ROCKSDB

#include "TableStorage.h"
#include "RocksDBStorage.h"
#include "lingodb/catalog/TableCatalogEntry.h"
#include "lingodb/runtime/ArrowView.h"

#include <functional>
#include <string>
#include <memory>
#include <arrow/table.h>
#include <arrow/record_batch.h>

namespace lingodb::runtime {

/**
 * RocksDB-based implementation of TableStorage interface
 * Stores Arrow data in RocksDB with efficient serialization/deserialization
 */
class RocksDBTableStorage : public TableStorage {
public:
    // Column buffer representation for direct RocksDB access
    struct ColumnBuffer {
        std::string data;           // Raw buffer data
        std::string validityBitmap; // Null bitmap if needed
        size_t numRows;
        size_t chunkId;
    };

private:
    std::shared_ptr<RocksDBStorage> storage;
    std::string tableName;
    std::shared_ptr<arrow::Schema> schema;
    catalog::Sample sample;
    size_t numRows;
    size_t numChunks;
    static constexpr size_t CHUNK_SIZE = 65536; // 64K rows per chunk
    
    struct TransparentStringHasher : std::hash<std::string>, std::hash<std::string_view> {
        using is_transparent = void;
        using std::hash<std::string>::operator();
        using std::hash<std::string_view>::operator();
    };
    using ColumnStatisticsMap = std::unordered_map<std::string, catalog::ColumnStatistics, TransparentStringHasher, std::equal_to<>>;
    ColumnStatisticsMap columnStatistics;

public:
    RocksDBTableStorage(std::shared_ptr<RocksDBStorage> storage, 
                       const std::string& tableName, 
                       std::shared_ptr<arrow::Schema> schema);
    
    RocksDBTableStorage(std::shared_ptr<RocksDBStorage> storage,
                       const std::string& tableName,
                       std::shared_ptr<arrow::Schema> schema,
                       size_t numRows,
                       catalog::Sample sample,
                       ColumnStatisticsMap columnStatistics);

    // TableStorage interface implementation
    size_t nextRowId() override {
        return numRows;
    }
    
    std::unique_ptr<scheduler::Task> createScanTask(const ScanConfig& scanConfig) override;
    const catalog::Sample& getSample() const {
        return sample;
    }
    const catalog::ColumnStatistics& getColumnStatistics(std::string_view column) const;
    size_t getNumRows() const {
        return numRows;
    }
    
    void append(const std::vector<std::shared_ptr<arrow::RecordBatch>>& toAppend) override;
    void append(const std::shared_ptr<arrow::Table>& toAppend) override;
    std::shared_ptr<arrow::DataType> getColumnStorageType(std::string_view columnName) const override;
    
    // RocksDB-specific operations
    void flush();
    
    // Static factory methods
    static std::unique_ptr<RocksDBTableStorage> create(std::shared_ptr<RocksDBStorage> storage,
                                                      const catalog::CreateTableDef& def);
    
    // Serialization support for metadata
    void serializeMetadata() const;
    void deserializeMetadata() const;

private:
    // RocksDB key generation
    std::string getColumnKey(const std::string& columnName, size_t chunkId) const;
    std::string getMetadataKey() const;
    std::string getStatisticsKey() const;
    std::string getSampleKey() const;
    
    // Column-oriented storage methods
    void storeColumn(const std::string& columnName, size_t chunkId, 
                    const std::shared_ptr<arrow::Array>& array);
    std::shared_ptr<arrow::Array> loadColumn(const std::string& columnName, 
                                            size_t chunkId) const;
    ColumnBuffer loadColumnBuffer(const std::string& columnName, size_t chunkId) const;
    
    // Batch operations - need to be public for scan task
public:
    std::shared_ptr<arrow::RecordBatch> loadRecordBatch(size_t chunkId) const;
private:
    void storeRecordBatch(size_t chunkId, const std::shared_ptr<arrow::RecordBatch>& batch);
    
    // Statistics management  
    void updateStatistics(const std::shared_ptr<arrow::Table>& table);
    
    // Utility methods
    size_t getChunkForRow(size_t rowId) const;
    size_t getRowOffsetInChunk(size_t rowId) const;
};

} // namespace lingodb::runtime

#endif // WITH_ROCKSDB

#endif // LINGODB_RUNTIME_STORAGE_ROCKSDBTABLESTORAGE_H 