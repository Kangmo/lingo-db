add_library(runtime OBJECT
        Tracing.cpp
        ArrowColumn.cpp
        ArrowTable.cpp
        RecordBatchInfo.cpp
        ThreadLocal.cpp
        Heap.cpp
        DumpRuntime.cpp
        DataSourceIteration.cpp
        Sorting.cpp
        GrowingBuffer.cpp
        Buffer.cpp
        SimpleState.cpp
        LazyJoinHashtable.cpp
        SegmentTreeView.cpp
        Hashtable.cpp
        PreAggregationHashtable.cpp
        HashMultiMap.cpp
        StringRuntime.cpp
        FloatRuntime.cpp
        DecimalRuntime.cpp
        IntegerRuntime.cpp
        Hash.cpp
        Timing.cpp
        DateRuntime.cpp
        ExecutionContext.cpp
        RelationHelper.cpp
        EntryLock.cpp
        #Database.cpp
        #ArrowDirDatabase.cpp
        #ExternalArrowDatabase.cpp
        #MetaDataOnlyDatabase.cpp
        #ExternalHashIndex.cpp
        LingoDBHashIndex.cpp
        Session.cpp
        storage/LingoDBTable.cpp
        storage/RocksDBStorage.cpp
)
set(COMPILE_DEFS "")
if (ENABLE_GPU_BACKEND)
    list(APPEND COMPILE_DEFS "GPU_ENABLED=1")
else ()
    list(APPEND COMPILE_DEFS "GPU_ENABLED=0")
endif (ENABLE_GPU_BACKEND)

target_compile_definitions(runtime PUBLIC ${COMPILE_DEFS})
# Ensure Zstandard (zstd) is found for RocksDB dependency
find_package(zstd)
# Workaround: Provide dummy zstd::zstd target if missing (for RocksDB CMake compatibility)
if(NOT TARGET zstd::zstd)
    add_library(zstd::zstd INTERFACE IMPORTED)
    set_target_properties(zstd::zstd PROPERTIES INTERFACE_LINK_LIBRARIES zstd)
endif()
find_package(RocksDB REQUIRED)
if(TARGET zstd::zstd)
    target_link_libraries(runtime PRIVATE Arrow::arrow_static zstd::zstd RocksDB::rocksdb)
else()
    target_link_libraries(runtime PRIVATE Arrow::arrow_static zstd RocksDB::rocksdb)
endif()

add_subdirectory(GPU)
target_link_libraries(runtime PUBLIC GPU-rt scheduler)
