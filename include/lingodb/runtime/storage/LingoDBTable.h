#pragma once

#include <arrow/api.h>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "lingodb/catalog/Defs.h"
#include "lingodb/catalog/MetaData.h"
#include "lingodb/runtime/RecordBatchInfo.h"
#include "lingodb/runtime/storage/TableStorage.h"
#include "lingodb/scheduler/Tasks.h"
#include "lingodb/utility/Serialization.h"

namespace lingodb::runtime {

class LingoDBTable : public TableStorage {
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

   static std::unique_ptr<LingoDBTable> create(const catalog::CreateTableDef& def);
   static std::unique_ptr<LingoDBTable> deserialize(lingodb::utility::Deserializer& deserializer);

   LingoDBTable(std::string fileName, std::shared_ptr<arrow::Schema> arrowSchema);
   ~LingoDBTable();

   // TableStorage interface implementation
   void append(const std::shared_ptr<arrow::Table>& table) override;
   void append(const std::vector<std::shared_ptr<arrow::RecordBatch>>& toAppend) override;
   std::shared_ptr<arrow::DataType> getColumnStorageType(const std::string& columnName) const override;
   std::unique_ptr<scheduler::Task> createScanTask(const ScanConfig& scanConfig) override;
   size_t nextRowId() override { return getNumRows(); }
   std::pair<const runtime::TableChunk*, size_t> getByRowId(size_t rowId) const override;
   size_t getColIndex(const std::string& colName) override;

   // Additional LingoDBTable methods
   void flush();
   const catalog::ColumnStatistics& getColumnStatistics(std::string column) const;
   void ensureLoaded();
   // Removed serialize method to avoid custom serialization of Arrow types
   
   // Methods needed by TableCatalogEntry
   void setPersist(bool shouldPersist);
   void setDBDir(const std::string& dbDir);
   size_t getNumRows() const;
   const catalog::Sample& getSample() const;

   private:
   class Impl;
   std::unique_ptr<Impl> impl;
   std::string fileName;
   catalog::Sample sample;
};

} // namespace lingodb::runtime
