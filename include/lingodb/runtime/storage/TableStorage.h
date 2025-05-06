#ifndef LINGODB_RUNTIME_STORAGE_TABLESTORAGE_H
#define LINGODB_RUNTIME_STORAGE_TABLESTORAGE_H
#include "lingodb/runtime/RecordBatchInfo.h"
#include "lingodb/scheduler/Task.h"

#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include <arrow/api.h>

namespace lingodb::runtime {
struct ScanConfig {
   bool parallel;
   std::vector<std::string> columns;
   std::function<void(lingodb::runtime::RecordBatchInfo*)> cb;
};

// Common TableChunk definition for all storage implementations
struct TableChunk {
   virtual std::shared_ptr<arrow::RecordBatch> data() const = 0;
   virtual size_t getNumRows() const = 0;
   virtual ~TableChunk() = default;
};

class TableStorage {
   public:
   virtual std::shared_ptr<arrow::DataType> getColumnStorageType(const std::string& columnName) const = 0;
   virtual std::unique_ptr<scheduler::Task> createScanTask(const ScanConfig& scanConfig) = 0;
   virtual void append(const std::vector<std::shared_ptr<arrow::RecordBatch>>& toAppend) = 0;
   virtual size_t nextRowId() = 0;
   virtual void append(const std::shared_ptr<arrow::Table>& toAppend) = 0;
   virtual std::pair<const TableChunk*, size_t> getByRowId(size_t rowId) const = 0;
   virtual size_t getColIndex(const std::string& colName) = 0;
   virtual ~TableStorage() = default;
};
} // namespace lingodb::runtime
#endif //LINGODB_RUNTIME_STORAGE_TABLESTORAGE_H
