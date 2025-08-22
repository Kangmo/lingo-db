#ifndef LINGODB_RUNTIME_STORAGE_COLUMNFAMILYHANDLE_H
#define LINGODB_RUNTIME_STORAGE_COLUMNFAMILYHANDLE_H

#ifdef WITH_ROCKSDB
#include <rocksdb/db.h>
#include <memory>
#include <vector>

namespace lingodb::runtime {

/**
 * RAII wrapper for RocksDB column family handles
 * Ensures proper cleanup even in case of exceptions
 */
class ColumnFamilyHandleWrapper {
private:
    rocksdb::ColumnFamilyHandle* handle;
    
public:
    explicit ColumnFamilyHandleWrapper(rocksdb::ColumnFamilyHandle* h = nullptr) 
        : handle(h) {}
    
    ~ColumnFamilyHandleWrapper() {
        reset();
    }
    
    // Disable copy
    ColumnFamilyHandleWrapper(const ColumnFamilyHandleWrapper&) = delete;
    ColumnFamilyHandleWrapper& operator=(const ColumnFamilyHandleWrapper&) = delete;
    
    // Enable move
    ColumnFamilyHandleWrapper(ColumnFamilyHandleWrapper&& other) noexcept 
        : handle(other.handle) {
        other.handle = nullptr;
    }
    
    ColumnFamilyHandleWrapper& operator=(ColumnFamilyHandleWrapper&& other) noexcept {
        if (this != &other) {
            reset();
            handle = other.handle;
            other.handle = nullptr;
        }
        return *this;
    }
    
    void reset(rocksdb::ColumnFamilyHandle* h = nullptr) {
        if (handle) {
            delete handle;
        }
        handle = h;
    }
    
    rocksdb::ColumnFamilyHandle* get() const {
        return handle;
    }
    
    rocksdb::ColumnFamilyHandle* release() {
        auto* h = handle;
        handle = nullptr;
        return h;
    }
    
    rocksdb::ColumnFamilyHandle* operator->() const {
        return handle;
    }
    
    rocksdb::ColumnFamilyHandle& operator*() const {
        return *handle;
    }
    
    explicit operator bool() const {
        return handle != nullptr;
    }
};

/**
 * Container for multiple column family handles with RAII cleanup
 */
class ColumnFamilyHandles {
private:
    std::vector<std::unique_ptr<ColumnFamilyHandleWrapper>> handles;
    
public:
    ColumnFamilyHandles() = default;
    
    void add(rocksdb::ColumnFamilyHandle* handle) {
        handles.push_back(std::make_unique<ColumnFamilyHandleWrapper>(handle));
    }
    
    void addAll(std::vector<rocksdb::ColumnFamilyHandle*>& rawHandles) {
        for (auto* handle : rawHandles) {
            add(handle);
        }
        rawHandles.clear(); // Clear the source vector as we've taken ownership
    }
    
    rocksdb::ColumnFamilyHandle* get(size_t index) const {
        if (index < handles.size() && handles[index]) {
            return handles[index]->get();
        }
        return nullptr;
    }
    
    size_t size() const {
        return handles.size();
    }
    
    void clear() {
        handles.clear();
    }
    
    // Get raw handles for RocksDB API calls
    std::vector<rocksdb::ColumnFamilyHandle*> getRawHandles() const {
        std::vector<rocksdb::ColumnFamilyHandle*> raw;
        raw.reserve(handles.size());
        for (const auto& wrapper : handles) {
            if (wrapper) {
                raw.push_back(wrapper->get());
            }
        }
        return raw;
    }
};

} // namespace lingodb::runtime

#endif // WITH_ROCKSDB

#endif // LINGODB_RUNTIME_STORAGE_COLUMNFAMILYHANDLE_H