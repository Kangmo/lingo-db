#ifdef WITH_ROCKSDB

#include "lingodb/catalog/RocksDBCatalog.h"
#include "lingodb/catalog/IndexCatalogEntry.h"
#include "lingodb/catalog/TableCatalogEntry.h"
#include "lingodb/catalog/Types.h"
#include "lingodb/catalog/Catalog.h"
#include "lingodb/utility/Serialization.h"
#include "lingodb/runtime/storage/RocksDBStorage.h"
#include "lingodb/runtime/storage/RocksDBTableStorage.h"

#include <arrow/type.h>
#include <arrow/table.h>
#include <filesystem>
#include <sstream>
#include <iostream>

namespace lingodb::catalog {

RocksDBCatalog::RocksDBCatalog() : Catalog() {}

RocksDBCatalog::RocksDBCatalog(std::shared_ptr<lingodb::runtime::RocksDBStorage> storage) 
    : Catalog(), storage(storage) {
    if (storage) {
        dbDir = "";  // Will be set by caller
        initializeStorage();
    }
}

RocksDBCatalog::~RocksDBCatalog() {
    persist();
}

void RocksDBCatalog::serialize(lingodb::utility::Serializer& serializer) const {
    serializer.writeProperty(0, binaryVersion);
    
    // We don't serialize all entries here since they're stored individually in RocksDB
    // Just serialize the count for compatibility
    loadAllEntries();
    serializer.writeProperty(1, entryCache.size());
}

RocksDBCatalog RocksDBCatalog::deserialize(lingodb::utility::Deserializer& deserializer) {
    auto version = deserializer.readProperty<size_t>(0);
    if (version != binaryVersion) {
        throw std::runtime_error("RocksDBCatalog: version mismatch");
    }
    
    RocksDBCatalog res;
    // Entries will be loaded lazily from RocksDB
    (void)deserializer.readProperty<size_t>(1); // Read but don't use
    return res;
}

std::optional<std::shared_ptr<CatalogEntry>> RocksDBCatalog::getEntry(std::string name) {
    
    if (!storage) {
        return std::nullopt;
    }
    
    // Check cache first
    if (cacheValid && entryCache.contains(name)) {
        return entryCache.at(name);
    }
    
    
    // Load from RocksDB
    if (entryExists(name)) {
        try {
            auto entry = loadEntry(name);
            if (entry) {
                entryCache[name] = entry;
                return entry;
            } else {
            }
        } catch (const std::exception& e) {
        }
    } else {
    }
    
    return std::nullopt;
}

void RocksDBCatalog::persist() {
    if (!shouldPersist || !storage) {
        return;
    }
    
    // Persist all cached entries
    for (const auto& [name, entry] : entryCache) {
        entry->flush();
        storeEntry(name, entry);
    }
    
    // Flush the catalog column family
    auto status = storage->flush(lingodb::runtime::RocksDBStorage::ColumnFamily::CATALOG);
    if (!status.ok()) {
        throw std::runtime_error("Failed to flush catalog: " + status.ToString());
    }
}

void RocksDBCatalog::setShouldPersist(bool shouldPersist) {
    this->shouldPersist = shouldPersist;
    
    // Propagate to all cached entries
    for (auto& [name, entry] : entryCache) {
        entry->setShouldPersist(shouldPersist);
    }
}

void RocksDBCatalog::insertEntry(std::shared_ptr<CatalogEntry> entry) {
    if (!entry) {
        return;
    }
    
    std::string name = entry->getName();
    
    if (entryCache.contains(name) || entryExists(name)) {
        throw std::runtime_error("catalog entry already exists: " + name);
    }
    
    // Set up the entry
    entry->setCatalog(reinterpret_cast<Catalog*>(this));  // Cast for compatibility
    entry->setDBDir(dbDir);
    entry->setShouldPersist(shouldPersist);
    
    // Cache the entry
    entryCache[name] = entry;
    
    // Store in RocksDB
    if (storage) {
        try {
            storeEntry(name, entry);
        } catch (const std::exception& e) {
            throw;
        }
    }
}

std::shared_ptr<RocksDBCatalog> RocksDBCatalog::create(const std::string& dbDir, bool eagerLoading) {
    // Normalize the path to absolute
    std::filesystem::path normalizedPath = std::filesystem::absolute(dbDir);
    std::string absolutePath = normalizedPath.string();
    
    if (!std::filesystem::exists(absolutePath)) {
        std::filesystem::create_directories(absolutePath);
    }
    
    // Create or open RocksDB storage with absolute path
    auto storage = std::make_shared<lingodb::runtime::RocksDBStorage>(absolutePath);
    auto status = storage->open();
    if (!status.ok()) {
        throw std::runtime_error("Failed to open RocksDB: " + status.ToString());
    }
    
    auto catalog = std::make_shared<RocksDBCatalog>(storage);
    catalog->dbDir = absolutePath;
    catalog->checkVersion();
    catalog->setShouldPersist(true);  // Force persistence for RocksDB catalogs
    
    if (eagerLoading) {
        catalog->loadAllEntries();
        for (auto& [name, entry] : catalog->entryCache) {
            entry->setDBDir(absolutePath);
            entry->setCatalog(catalog.get());
            entry->ensureFullyLoaded();
        }
    }
    
    return catalog;
}

std::shared_ptr<RocksDBCatalog> RocksDBCatalog::createEmpty() {
    return std::make_shared<RocksDBCatalog>();
}

// Private methods

std::string RocksDBCatalog::getEntryKey(const std::string& entryName) const {
    return "entry:" + entryName;
}

std::string RocksDBCatalog::getVersionKey() const {
    return "catalog:version";
}

std::string RocksDBCatalog::serializeEntry(const std::shared_ptr<CatalogEntry>& entry) const {
    lingodb::utility::SimpleByteWriter writer;
    lingodb::utility::Serializer serializer(writer);
    entry->serialize(serializer);
    
    std::string result;
    result.reserve(writer.size());
    for (size_t i = 0; i < writer.size(); ++i) {
        result.push_back(static_cast<char>(writer.data()[i]));
    }
    return result;
}

std::shared_ptr<CatalogEntry> RocksDBCatalog::deserializeEntry(const std::string& data) const {
    std::vector<std::byte> bytes;
    bytes.reserve(data.size());
    for (char c : data) {
        bytes.push_back(static_cast<std::byte>(c));
    }
    
    lingodb::utility::SimpleByteReader reader(bytes.data(), bytes.size());
    lingodb::utility::Deserializer deserializer(reader);
    
    // For RocksDBCatalog, we need to handle RocksDBTableCatalogEntry specially
    // since it needs access to the storage instance
    auto entryType = deserializer.readProperty<CatalogEntry::CatalogEntryType>(1);
    switch (entryType) {
        case CatalogEntry::CatalogEntryType::INVALID_ENTRY:
            return nullptr;
        case CatalogEntry::CatalogEntryType::LINGODB_TABLE_ENTRY: {
            // This is actually a RocksDBTableCatalogEntry when in RocksDBCatalog
            // We need to deserialize it specially
            auto name = deserializer.readProperty<std::string>(2);
            auto columnCount = deserializer.readProperty<size_t>(3);
            std::vector<Column> columns;
            for (size_t i = 0; i < columnCount; i++) {
                columns.push_back(deserializer.readProperty<Column>(4));
            }
            auto primaryKey = deserializer.readProperty<std::vector<std::string>>(8);
            auto indices = deserializer.readProperty<std::vector<std::string>>(9);
            auto tableName = deserializer.readProperty<std::string>(10);
            
            // Create Arrow schema from columns using the same approach as RocksDBTableStorage::create
            // Define toPhysicalType function locally to convert lingodb types to Arrow types
            auto toPhysicalType = [](lingodb::catalog::Type t) -> std::shared_ptr<arrow::DataType> {
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
                            case lingodb::catalog::IntervalTypeInfo::IntervalUnit::DAYTIME:
                                return arrow::day_time_interval();
                            case lingodb::catalog::IntervalTypeInfo::IntervalUnit::MONTH:
                                return arrow::month_interval();
                            default:
                                return arrow::day_time_interval(); // Default to day_time_interval
                        }
                        break;
                    }
                    case TypeId::CHAR: {
                        auto charInfo = t.getInfo<lingodb::catalog::CharTypeInfo>();
                        if (charInfo) {
                            return arrow::fixed_size_binary(charInfo->getLength());
                        }
                        return arrow::utf8();
                        break;
                    }
                    case TypeId::STRING:
                        return arrow::utf8();
                        break;
                    default:
                        throw std::runtime_error("unsupported type");
                }
            };
            
            std::vector<std::shared_ptr<arrow::Field>> fields;
            for (const auto& column : columns) {
                fields.push_back(arrow::field(std::string{column.getColumnName()}, toPhysicalType(column.getLogicalType())));
            }
            auto schema = arrow::schema(fields);
            
            // Create RocksDBTableStorage for existing table
            auto impl = std::make_unique<runtime::RocksDBTableStorage>(storage, tableName, schema);
            impl->deserializeMetadata(); // Load existing metadata from RocksDB
            auto entry = std::make_shared<RocksDBTableCatalogEntry>(name, columns, primaryKey, 
                                                                   indices, std::move(impl), storage);
            return entry;
        }
        case CatalogEntry::CatalogEntryType::LINGODB_HASH_INDEX_ENTRY:
            return LingoDBHashIndexEntry::deserialize(deserializer);
        default:
            return nullptr;
    }
}

void RocksDBCatalog::loadAllEntries() const {
    if (cacheValid || !storage) {
        return;
    }
    
    entryCache.clear();
    auto entryNames = getAllEntryNames();
    
    for (const std::string& name : entryNames) {
        auto entry = loadEntry(name);
        if (entry) {
            entryCache[name] = entry;
            entry->setDBDir(dbDir);
            entry->setCatalog(const_cast<Catalog*>(reinterpret_cast<const Catalog*>(this)));
        }
    }
    
    cacheValid = true;
}

void RocksDBCatalog::invalidateCache() const {
    cacheValid = false;
    entryCache.clear();
}

void RocksDBCatalog::storeEntry(const std::string& name, const std::shared_ptr<CatalogEntry>& entry) {
    if (!storage) {
        return;
    }
    
    std::string key = getEntryKey(name);
    std::string value = serializeEntry(entry);
    
    auto status = storage->put(lingodb::runtime::RocksDBStorage::ColumnFamily::CATALOG, key, value);
    if (!status.ok()) {
        throw std::runtime_error("Failed to store catalog entry '" + name + "': " + status.ToString());
    }
}

std::shared_ptr<CatalogEntry> RocksDBCatalog::loadEntry(const std::string& name) const {
    if (!storage) {
        return nullptr;
    }
    
    std::string key = getEntryKey(name);
    std::string value;
    
    auto status = storage->get(lingodb::runtime::RocksDBStorage::ColumnFamily::CATALOG, key, &value);
    if (!status.ok()) {
        return nullptr;
    }
    
    return deserializeEntry(value);
}

bool RocksDBCatalog::entryExists(const std::string& name) const {
    if (!storage) {
        return false;
    }
    
    std::string key = getEntryKey(name);
    return storage->exists(lingodb::runtime::RocksDBStorage::ColumnFamily::CATALOG, key);
}

void RocksDBCatalog::removeEntry(const std::string& name) {
    if (!storage) {
        return;
    }
    
    std::string key = getEntryKey(name);
    auto status = storage->del(lingodb::runtime::RocksDBStorage::ColumnFamily::CATALOG, key);
    if (!status.ok()) {
        throw std::runtime_error("Failed to remove catalog entry '" + name + "': " + status.ToString());
    }
    
    // Remove from cache
    entryCache.erase(name);
}

std::vector<std::string> RocksDBCatalog::getAllEntryNames() const {
    if (!storage) {
        return {};
    }
    
    std::vector<std::string> names;
    auto iterator = storage->newIterator(lingodb::runtime::RocksDBStorage::ColumnFamily::CATALOG);
    std::string prefix = "entry:";
    
    iterator->Seek(prefix);
    while (iterator->Valid()) {
        std::string key = iterator->key().ToString();
        if (key.substr(0, prefix.length()) != prefix) {
            break;
        }
        
        // Extract entry name from key
        std::string entryName = key.substr(prefix.length());
        names.push_back(entryName);
        
        iterator->Next();
    }
    
    return names;
}

void RocksDBCatalog::initializeStorage() {
    if (!storage) {
        return;
    }
    
    // Check if this is a new database by looking for version key
    if (!storage->exists(lingodb::runtime::RocksDBStorage::ColumnFamily::CATALOG, getVersionKey())) {
        setVersion();
    } else {
        checkVersion();
    }
}

void RocksDBCatalog::checkVersion() {
    if (!storage) {
        return;
    }
    
    std::string versionValue;
    auto status = storage->get(lingodb::runtime::RocksDBStorage::ColumnFamily::CATALOG, getVersionKey(), &versionValue);
    
    if (status.ok()) {
        size_t storedVersion = std::stoull(versionValue);
        if (storedVersion != binaryVersion) {
            throw std::runtime_error("RocksDBCatalog: version mismatch. Expected " + 
                                   std::to_string(binaryVersion) + ", got " + std::to_string(storedVersion));
        }
    }
}

void RocksDBCatalog::setVersion() {
    if (!storage) {
        return;
    }
    
    std::string versionValue = std::to_string(binaryVersion);
    auto status = storage->put(lingodb::runtime::RocksDBStorage::ColumnFamily::CATALOG, getVersionKey(), versionValue);
    if (!status.ok()) {
        throw std::runtime_error("Failed to set catalog version: " + status.ToString());
    }
}

} // namespace lingodb::catalog

#endif // WITH_ROCKSDB 