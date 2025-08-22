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
    class TableChunk {
        std::shared_ptr<arrow::RecordBatch> internalData;
        [[maybe_unused]] size_t startRowId;
        size_t numRows;
        std::vector<const void*> buffers;
        std::vector<ArrayView> columnInfo;

    public:
        TableChunk(std::shared_ptr<arrow::RecordBatch> data, size_t startRowId);

        const std::shared_ptr<arrow::RecordBatch>& data() const {
            return internalData;
        }
        const ArrayView* getArrayView(size_t colId) const {
            return &columnInfo[colId];
        }
        size_t getNumRows() const {
            return numRows;
        }
        friend class RocksDBTableStorage;
    };

private:
    std::shared_ptr<RocksDBStorage> storage;
    std::string tableName;
    std::shared_ptr<arrow::Schema> schema;
    catalog::Sample sample;
    size_t numRows;
    bool loaded = false;
    
    // In-memory cache for recently accessed chunks
    mutable std::vector<TableChunk> chunkCache;
    mutable bool cacheValid = false;
    
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
    void ensureLoaded() const;
    std::pair<const TableChunk*, size_t> getByRowId(size_t rowId) const;
    
    // Static factory methods
    static std::unique_ptr<RocksDBTableStorage> create(std::shared_ptr<RocksDBStorage> storage,
                                                      const catalog::CreateTableDef& def);
    
    // Serialization support for metadata
    void serializeMetadata() const;
    void deserializeMetadata() const;

private:
    // RocksDB key generation
    std::string getChunkKey(size_t chunkId) const;
    std::string getMetadataKey() const;
    std::string getStatisticsKey() const;
    std::string getSampleKey() const;
    
    // Arrow serialization helpers
    std::string serializeRecordBatch(const std::shared_ptr<arrow::RecordBatch>& batch) const;
    std::shared_ptr<arrow::RecordBatch> deserializeRecordBatch(const std::string& data) const;
    
    // Chunk management
    void storeChunk(size_t chunkId, const std::shared_ptr<arrow::RecordBatch>& batch);
    std::shared_ptr<arrow::RecordBatch> loadChunk(size_t chunkId) const;
    void updateStatistics(const std::shared_ptr<arrow::Table>& table);
    void invalidateCache() const;
    void loadAllChunks();
    
    // Utility methods
    size_t getChunkCount() const;
    std::vector<size_t> getChunkIds() const;
};

} // namespace lingodb::runtime

#endif // WITH_ROCKSDB

#endif // LINGODB_RUNTIME_STORAGE_ROCKSDBTABLESTORAGE_H 