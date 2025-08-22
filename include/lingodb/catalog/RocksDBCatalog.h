#ifndef LINGODB_CATALOG_ROCKSDBCATALOG_H
#define LINGODB_CATALOG_ROCKSDBCATALOG_H

#ifdef WITH_ROCKSDB

#include "Catalog.h"
#include "lingodb/runtime/storage/RocksDBStorage.h"
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>

namespace lingodb::utility {
class Serializer;
class Deserializer;
} // namespace lingodb::utility

namespace lingodb::catalog {

/**
 * RocksDB-based implementation of the Catalog interface
 * Stores catalog entries in RocksDB instead of a single file
 */
class RocksDBCatalog : public Catalog {
    static constexpr size_t binaryVersion = 2;
    std::shared_ptr<lingodb::runtime::RocksDBStorage> storage;

public:
    RocksDBCatalog();
    explicit RocksDBCatalog(std::shared_ptr<lingodb::runtime::RocksDBStorage> storage);
    
    // Move constructor and move assignment operator
    RocksDBCatalog(RocksDBCatalog&& other) noexcept;
    RocksDBCatalog& operator=(RocksDBCatalog&& other) noexcept;
    
    // Delete copy constructor and copy assignment operator
    RocksDBCatalog(const RocksDBCatalog&) = delete;
    RocksDBCatalog& operator=(const RocksDBCatalog&) = delete;
    
    void serialize(lingodb::utility::Serializer& serializer) const;
    static RocksDBCatalog deserialize(lingodb::utility::Deserializer& deSerializer);

    // Override Catalog virtual methods
    bool hasRocksDBSupport() const override { return storage != nullptr; }
    std::shared_ptr<lingodb::runtime::RocksDBStorage> getRocksDBStorage() const override { return storage; }

    std::optional<std::shared_ptr<CatalogEntry>> getEntry(std::string name) override;
    
    // Override getTypedEntry to use our RocksDB-based getEntry() method
    template <class T>
    std::optional<std::shared_ptr<T>> getTypedEntry(std::string name) {
        auto entry = getEntry(name);
        if (entry.has_value()) {
            for (auto x : T::entryTypes) {
                if (entry.value()->getEntryType() == x) {
                    return std::static_pointer_cast<T>(entry.value());
                }
            }
        }
        return std::nullopt;
    }

    std::shared_ptr<RocksDBCatalog> getRocksDBCatalog();

    // Static factory methods
    static std::shared_ptr<RocksDBCatalog> create(const std::string& dbDir, bool eagerLoading = false);
    static std::shared_ptr<RocksDBCatalog> createEmpty();

    void persist() override;
    void setShouldPersist(bool shouldPersist);
    void insertEntry(std::shared_ptr<CatalogEntry> entry) override;
    
    ~RocksDBCatalog();

private:
    // Thread-safe in-memory cache of catalog entries for performance
    mutable std::shared_mutex cacheMutex;
    mutable std::unordered_map<std::string, std::shared_ptr<CatalogEntry>> entryCache;
    mutable bool cacheValid = false;
    
    // RocksDB key generation
    std::string getEntryKey(const std::string& entryName) const;
    std::string getVersionKey() const;
    
    // Entry serialization/deserialization
    std::string serializeEntry(const std::shared_ptr<CatalogEntry>& entry) const;
    std::shared_ptr<CatalogEntry> deserializeEntry(const std::string& data) const;
    
    // Cache management
    void loadAllEntries() const;
    void invalidateCache() const;
    
    // Storage operations
    void storeEntry(const std::string& name, const std::shared_ptr<CatalogEntry>& entry);
    std::shared_ptr<CatalogEntry> loadEntry(const std::string& name) const;
    bool entryExists(const std::string& name) const;
    void removeEntry(const std::string& name);
    std::vector<std::string> getAllEntryNames() const;
    
    // Initialization
    void initializeStorage();
    void checkVersion();
    void setVersion();
};

} // namespace lingodb::catalog

#endif // WITH_ROCKSDB

#endif // LINGODB_CATALOG_ROCKSDBCATALOG_H 