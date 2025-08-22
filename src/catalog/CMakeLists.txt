add_library(mlirTypes MLIRTypes.cpp)
target_link_libraries(mlirTypes
        PUBLIC ${dialect_libs}
        ${conversion_libs}
        ${translation_libs}
)

set(CATALOG_SOURCES Catalog.cpp Types.cpp Defs.cpp TableCatalogEntry.cpp IndexCatalogEntry.cpp MetaData.cpp Column.cpp)
if(RocksDB_FOUND)
    list(APPEND CATALOG_SOURCES RocksDBCatalog.cpp)
endif()

add_library(catalog ${CATALOG_SOURCES})
target_link_libraries(catalog PUBLIC mlirTypes)
if(RocksDB_FOUND)
    target_link_libraries(catalog PUBLIC RocksDB::rocksdb)
endif()