# Find Catch2 (CMake >= 3.11 recommended)
find_package(Catch2 REQUIRED)
include_directories(CATCH2_INCLUDE_DIR)
include_directories(${CMAKE_SOURCE_DIR}/src/runtime/storage)
add_executable(tester
        catalog/TestCatalog.cpp
        catalog/TestTypes.cpp
        catalog/TestMetaData.cpp
        catalog/TestCatalogEntries.cpp
        storage/TestStorage.cpp
        storage/TestRocksDBStorage.cpp
        utility/TestSerialization.cpp
)
target_link_libraries(tester PRIVATE Catch2::Catch2WithMain ${LIBS} utility catalog MLIRSQLFrontend runtime runner)
set_target_properties(tester PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")

enable_testing()
add_test(NAME UnitTests COMMAND tester)
