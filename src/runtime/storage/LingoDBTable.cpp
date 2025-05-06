#include "lingodb/runtime/storage/LingoDBTable.h"
#include "lingodb/runtime/storage/RocksDBStorage.h"
#include "lingodb/catalog/Defs.h"
#include "lingodb/runtime/RecordBatchInfo.h"
#include "lingodb/scheduler/Tasks.h"
#include "lingodb/utility/Serialization.h"
#include "lingodb/utility/Tracer.h"

#include <arrow/builder.h>
#include <arrow/compute/api_vector.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/table.h>

#include <algorithm>
#include <filesystem>
#include <iostream>
#include <random>
#include <ranges>
namespace {
namespace utility = lingodb::utility;
static utility::Tracer::Event processMorsel("DataSourceIteration", "processMorsel");

static utility::Tracer::Event processMorselSingle("DataSourceIteration", "processMorselSingle");

static utility::Tracer::Event cleanupTLS("DataSourceIteration", "cleanup");
std::vector<lingodb::runtime::LingoDBTable::TableChunk> loadTable(std::string name) {
   auto inputFile = arrow::io::ReadableFile::Open(name).ValueOrDie();
   auto batchReader = arrow::ipc::RecordBatchFileReader::Open(inputFile).ValueOrDie();
   std::vector<lingodb::runtime::LingoDBTable::TableChunk> batches;
   size_t currRowId = 0;
   for (int i = 0; i < batchReader->num_record_batches(); i++) {
      auto batch = batchReader->ReadRecordBatch(i).ValueOrDie();
      batches.push_back(lingodb::runtime::LingoDBTable::TableChunk(batch, currRowId));
      currRowId += batch->num_rows();
   }
   return batches;
}
void storeTable(std::string file, std::shared_ptr<arrow::Schema> schema, const std::vector<lingodb::runtime::LingoDBTable::TableChunk>& data) {
   auto inputFile = arrow::io::FileOutputStream::Open(file).ValueOrDie();
   auto batchWriter = arrow::ipc::MakeFileWriter(inputFile, schema).ValueOrDie();
   for (auto& batch : data) {
      if (!batchWriter->WriteRecordBatch(*batch.data()).ok()) {
         throw std::runtime_error("could not store table");
      }
   }
   auto close1 = batchWriter->Close();
   if (!close1.ok()) {
      throw std::runtime_error("could not store table:" + close1.ToString());
   }
   if (!inputFile->Close().ok()) {
      throw std::runtime_error("could not store table");
   }
}
/*
 * Create sample from arrow table
 */

template <typename I>
class BoxedIntegerIterator {
   I i;

   public:
   typedef I difference_type;
   typedef I value_type;
   typedef I pointer;
   typedef I reference;
   typedef std::random_access_iterator_tag iterator_category;

   BoxedIntegerIterator(I i) : i{i} {}

   bool operator==(BoxedIntegerIterator<I>& other) { return i == other.i; }
   I operator-(BoxedIntegerIterator<I>& other) { return i - other.i; }
   I operator++() { return i++; }
   I operator*() { return i; }
};
std::shared_ptr<arrow::RecordBatch> createSample(const std::vector<lingodb::runtime::LingoDBTable::TableChunk>& data) {
   size_t numRows = 0;
   for (auto& batch : data) {
      numRows += batch.data()->num_rows();
   }
   if (numRows == 0) {
      return std::shared_ptr<arrow::RecordBatch>();
   }
   std::vector<size_t> result;

   auto rng = std::mt19937{std::random_device{}()};

   // sample five values without replacement from [1, 100]
   std::sample(
      BoxedIntegerIterator<size_t>{0l}, BoxedIntegerIterator<size_t>{numRows},
      std::back_inserter(result), std::min<size_t>(numRows, 1024ull), rng);
   std::sort(result.begin(), result.end());
   size_t currPos = 0;
   size_t currBatch = 0;
   size_t batchStart = 0;
   std::vector<std::shared_ptr<arrow::RecordBatch>> sampleData;
   while (currPos < result.size()) {
      std::vector<size_t> fromCurrentBatch;
      while (currPos < result.size() && result[currPos] < batchStart + data[currBatch].data()->num_rows()) {
         fromCurrentBatch.push_back(result[currPos] - batchStart);
         currPos++;
      }
      if (fromCurrentBatch.size() > 0) {
         arrow::NumericBuilder<arrow::Int32Type> numericBuilder;
         for (auto i : fromCurrentBatch) {
            if (!numericBuilder.Append(i).ok()) {
               throw std::runtime_error("could not create sample");
            }
         }
         auto indices = numericBuilder.Finish().ValueOrDie();
         std::vector<arrow::Datum> args({data[currBatch].data(), indices});
         auto res = arrow::compute::Take(args[0], args[1]).ValueOrDie();
         sampleData.push_back(res.record_batch());
      }
      batchStart += data[currBatch].data()->num_rows();
      currBatch++;
   }
   return arrow::Table::FromRecordBatches(sampleData).ValueOrDie()->CombineChunksToBatch().ValueOrDie();
}

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
               throw std::runtime_error("unsupported date unit");
         }
      }
      case TypeId::STRING:
      case TypeId::CHAR:
         return arrow::utf8();
      default:
         std::cerr << "[LingoDBTable] Unsupported type encountered. TypeId: " << static_cast<int>(t.getTypeId()) << std::endl;
         throw std::runtime_error("unsupported type");
   }
}
} // namespace

namespace lingodb::runtime {

LingoDBTable::TableChunk::TableChunk(std::shared_ptr<arrow::RecordBatch> data, size_t startRowId)
    : internalData(data), startRowId(startRowId), numRows(data->num_rows()) {
   for (auto colId = 0; colId < data->num_columns(); colId++) {
      columnInfo.push_back(RecordBatchInfo::getColumnInfo(colId, data));
   }
}

class LingoDBTable::Impl {
private:
   std::unique_ptr<RocksDBStorage> storage;
   bool persist = true;
   std::string dbDir;

public:
   Impl(std::string fileName, std::shared_ptr<arrow::Schema> arrowSchema) 
      : storage(std::make_unique<RocksDBStorage>(fileName, arrowSchema)) {}

   void append(const std::vector<std::shared_ptr<arrow::RecordBatch>>& toAppend) {
      storage->append(toAppend);
   }

   void append(const std::shared_ptr<arrow::Table>& table) {
      storage->append(table);
   }

   void flush() {
      storage->flush();
   }

   std::shared_ptr<arrow::Schema> getSchema() const {
      return storage->getSchema();
   }

   const catalog::ColumnStatistics& getColumnStatistics(const std::string& column) const {
      return storage->getColumnStatistics(column);
   }

   std::shared_ptr<arrow::DataType> getColumnStorageType(const std::string& columnName) const {
      return storage->getSchema()->GetFieldByName(columnName)->type();
   }

   void ensureLoaded() {
      storage->ensureLoaded();
   }

   std::pair<const RocksDBStorage::TableChunk*, size_t> getByRowId(size_t rowId) const {
      return storage->getByRowId(rowId);
   }

   size_t getColIndex(const std::string& colName) {
      return storage->getColIndex(colName);
   }

   std::unique_ptr<scheduler::Task> createScanTask(const ScanConfig& scanConfig) {
      return storage->createScanTask(scanConfig);
   }

   size_t getNumRows() const {
      return storage->getNumRows();
   }

   const catalog::Sample& getSample() const {
      return storage->getSample();
   }

   void setPersist(bool shouldPersist) {
      persist = shouldPersist;
   }

   void setDBDir(const std::string& dir) {
      dbDir = dir;
   }
};

std::unique_ptr<LingoDBTable> LingoDBTable::create(const catalog::CreateTableDef& def) {
   arrow::FieldVector fields;
   for (auto& c : def.columns) {
      fields.push_back(std::make_shared<arrow::Field>(c.getColumnName(), toPhysicalType(c.getLogicalType())));
   }
   auto arrowSchema = std::make_shared<arrow::Schema>(fields);
   std::string fileName = def.name + ".arrow";
   return std::make_unique<LingoDBTable>(fileName, arrowSchema);
}

LingoDBTable::LingoDBTable(std::string fileName, std::shared_ptr<arrow::Schema> arrowSchema)
    : impl(std::make_unique<Impl>(fileName, arrowSchema)), fileName(std::move(fileName)), sample(arrowSchema) {
}

LingoDBTable::~LingoDBTable() = default;

void LingoDBTable::append(const std::shared_ptr<arrow::Table>& table) {
   impl->append(table);
}

void LingoDBTable::append(const std::vector<std::shared_ptr<arrow::RecordBatch>>& toAppend) {
   impl->append(toAppend);
}

void LingoDBTable::flush() {
   impl->flush();
}

const catalog::ColumnStatistics& LingoDBTable::getColumnStatistics(std::string column) const {
   return impl->getColumnStatistics(column);
}

std::shared_ptr<arrow::DataType> LingoDBTable::getColumnStorageType(const std::string& columnName) const {
   return impl->getColumnStorageType(columnName);
}

void LingoDBTable::ensureLoaded() {
   impl->ensureLoaded();
}

std::unique_ptr<scheduler::Task> LingoDBTable::createScanTask(const ScanConfig& scanConfig) {
   return impl->createScanTask(scanConfig);
}

std::pair<const runtime::TableChunk*, size_t> LingoDBTable::getByRowId(size_t rowId) const {
   auto [chunk, offset] = impl->getByRowId(rowId);
   // Cast to the base interface type
   return {static_cast<const runtime::TableChunk*>(chunk), offset};
}

size_t LingoDBTable::getColIndex(const std::string& colName) {
   return impl->getColIndex(colName);
}

void LingoDBTable::setPersist(bool shouldPersist) {
   impl->setPersist(shouldPersist);
}

void LingoDBTable::setDBDir(const std::string& dbDir) {
   impl->setDBDir(dbDir);
}

size_t LingoDBTable::getNumRows() const {
   return impl->getNumRows();
}

const catalog::Sample& LingoDBTable::getSample() const {
   return impl->getSample();
}

// Serialization methods
std::unique_ptr<LingoDBTable> LingoDBTable::deserialize(utility::Deserializer& deserializer) {
   std::string fileName = deserializer.readProperty<std::string>(1);
   std::string schema_blob = deserializer.readProperty<std::string>(2);
   auto buffer = std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t*>(schema_blob.data()), schema_blob.size());
   arrow::io::BufferReader reader(buffer);
   auto result = arrow::ipc::ReadSchema(&reader, nullptr);
   if (!result.ok()) throw std::runtime_error(result.status().ToString());
   std::shared_ptr<arrow::Schema> schema = result.ValueOrDie();
   return std::make_unique<LingoDBTable>(fileName, schema);
}

} // namespace lingodb::runtime