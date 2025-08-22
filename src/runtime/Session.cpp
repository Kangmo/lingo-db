#include "lingodb/runtime/Session.h"
#include "lingodb/runtime/ExecutionContext.h"
#ifdef WITH_ROCKSDB
#include "lingodb/catalog/RocksDBCatalog.h"
#endif

#include <filesystem>
#include <iostream> // Added for debug output

std::unique_ptr<lingodb::runtime::ExecutionContext> lingodb::runtime::Session::createExecutionContext() {
   return std::make_unique<ExecutionContext>(*this);
}

std::shared_ptr<lingodb::catalog::Catalog> lingodb::runtime::Session::getCatalog() {
   return catalog;
}

std::shared_ptr<lingodb::runtime::Session> lingodb::runtime::Session::createSession() {
   return std::make_shared<Session>(catalog::Catalog::createEmpty());
}

std::shared_ptr<lingodb::runtime::Session> lingodb::runtime::Session::createSession(std::string dbDir, bool eagerLoading) {
#ifdef WITH_ROCKSDB
   // Always use RocksDB catalog when a directory is provided
   if (!dbDir.empty()) {
      try {
         auto rocksdbCatalog = catalog::RocksDBCatalog::create(dbDir, eagerLoading);
         if (rocksdbCatalog) {
            return std::make_shared<Session>(rocksdbCatalog);
         }
      } catch (const std::exception& e) {
         std::cerr << "Error creating RocksDB catalog: " << e.what() << std::endl;
         throw;
      }
   }
#endif
   // Only use standard catalog when no directory is provided
   return std::make_shared<Session>(catalog::Catalog::create(dbDir, eagerLoading));
}