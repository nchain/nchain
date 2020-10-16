
set( CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-function -Wno-sign-compare" )
set(LEVELDB_BUILD_TESTS OFF)
set(LEVELDB_BUILD_BENCHMARKS OFF)
set(LEVELDB_INSTALL OFF)
add_subdirectory( leveldb )
