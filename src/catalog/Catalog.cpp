#include "lingodb/catalog/Catalog.h"
#include "lingodb/catalog/IndexCatalogEntry.h"
#include "lingodb/catalog/TableCatalogEntry.h"
#include "lingodb/catalog/Types.h"
#include "lingodb/utility/Serialization.h"

#include <filesystem>

namespace lingodb::catalog {
Catalog Catalog::deserialize(lingodb::utility::Deserializer& deSerializer) {
   Catalog res;
   auto version = deSerializer.readProperty<size_t>(0);
   if (version != binaryVersion) {
      throw std::runtime_error("Catalog: version mismatch");
   }
   res.entries = deSerializer.readProperty<std::unordered_map<std::string, std::shared_ptr<CatalogEntry>>>(1);
   return res;
}
void Catalog::serialize(lingodb::utility::Serializer& serializer) const {
   serializer.writeProperty(0, binaryVersion);
   serializer.writeProperty(1, entries);
}

void CatalogEntry::serialize(lingodb::utility::Serializer& serializer) const {
   serializer.writeProperty(1, entryType);
   serializeEntry(serializer);
}
std::shared_ptr<CatalogEntry> CatalogEntry::deserialize(lingodb::utility::Deserializer& deserializer) {
   auto entryType = deserializer.readProperty<CatalogEntryType>(1);
   switch (entryType) {
      case CatalogEntryType::INVALID_ENTRY:
         return nullptr;
      case CatalogEntryType::LINGODB_TABLE_ENTRY:
         return LingoDBTableCatalogEntry::deserialize(deserializer);
      case CatalogEntryType::LINGODB_HASH_INDEX_ENTRY:
         return LingoDBHashIndexEntry::deserialize(deserializer);
   }
}

void Catalog::insertEntry(std::shared_ptr<CatalogEntry> entry) {
   if (entries.contains(entry->getName())) {
      throw std::runtime_error("catalog entry already exists");
   }
   entry->setCatalog(this);
   entry->setDBDir(dbDir);
   entry->setShouldPersist(shouldPersist);
   entries.insert({entry->getName(), std::move(entry)});
}

void Catalog::persist() {
   if (shouldPersist) {
      if (!std::filesystem::exists(dbDir)) {
         throw std::runtime_error("Catalog: dbDir does not exist");
      }
      for (auto& entry : entries) {
         entry.second->flush();
      }
      lingodb::utility::FileByteWriter reader(dbDir + "/db.lingodb");
      lingodb::utility::Serializer serializer(reader);
      serializer.writeProperty(0, *this);
   }
}
std::shared_ptr<Catalog> Catalog::create(std::string dbDir, bool eagerLoading) {
   // Normalize the path to absolute to avoid creating files in unexpected locations
   std::filesystem::path normalizedPath = std::filesystem::absolute(dbDir);
   std::string absolutePath = normalizedPath.string();
   
   if (!std::filesystem::exists(absolutePath)) {
      std::filesystem::create_directories(absolutePath);
   }
   if (!std::filesystem::exists(absolutePath + "/db.lingodb")) {
      auto res = std::make_shared<Catalog>();
      res->dbDir = absolutePath;
      return res;
   } else {
      lingodb::utility::FileByteReader reader(absolutePath + "/db.lingodb");
      lingodb::utility::Deserializer deserializer(reader);
      auto res = std::make_shared<Catalog>(deserializer.readProperty<Catalog>(0));
      res->dbDir = absolutePath;
      for (auto& entry : res->entries) {
         entry.second->setDBDir(absolutePath);
         entry.second->setCatalog(&*res);
      }
      if (eagerLoading) {
         for (auto& entry : res->entries) {
            entry.second->ensureFullyLoaded();
         }
      }
      return res;
   }
}
std::shared_ptr<Catalog> Catalog::createEmpty() {
   return std::make_shared<Catalog>();
}

} // namespace lingodb::catalog