cmake_minimum_required(VERSION 3.10)

find_program(CCACHE_FOUND ccache)
if (CCACHE_FOUND)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif (CCACHE_FOUND)

# set(CMAKE_TOOLCHAIN_FILE
#     "${CMAKE_SOURCE_DIR}/cmake/toolchain/cxx17.cmake"
#     CACHE
#     FILEPATH
#     "Default toolchain"
#     )

set( CMAKE_CXX_STANDARD 17 )
set( CMAKE_CXX_EXTENSIONS ON )
set( CXX_STANDARD_REQUIRED ON)

cmake_policy(SET CMP0048 NEW)
project(p2p VERSION 0.0.1 LANGUAGES C CXX)

set(LIBP2P_ROOT ${PROJECT_SOURCE_DIR}/libp2p )
set(LIBP2P_SRC_ROOT ${LIBP2P_ROOT}/src)

include(${LIBP2P_ROOT}/cmake/print.cmake)
print("C flags: ${CMAKE_C_FLAGS}")
print("CXX flags: ${CMAKE_CXX_FLAGS}")
# print("Using CMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}")

include(CheckCXXCompilerFlag)

set(${LIBP2P_ROOT}/cmake_EXPORT_COMPILE_COMMANDS ON)

## dependences

# https://docs.hunter.sh/en/latest/packages/pkg/Boost.html
find_package(Boost 1.71 REQUIRED random filesystem program_options)

# https://www.openssl.org/
# if(APPLE AND UNIX AND "${OPENSSL_ROOT_DIR}" STREQUAL "")
#    set(OPENSSL_ROOT_DIR "/usr/local/opt/openssl@1.1")
# endif()
# find_package(OpenSSL REQUIRED)

# https://developers.google.com/protocol-buffers
find_package(Protobuf 3.12.0 REQUIRED)

set_target_properties(protobuf::protoc PROPERTIES
IMPORTED_LOCATION_RELEASE "${Protobuf_LIBRARY_RELEASE}")

# https://github.com/gabime/spdlog
add_subdirectory(externals/spdlog)

# https://github.com/Tessil/hat-trie
add_subdirectory(externals/tsl_hat_trie)
if (NOT TARGET tsl::tsl_hat_trie)
  add_library(tsl::tsl_hat_trie ALIAS tsl_hat_trie)
endif()

# https://github.com/boost-ext/di
# git submodule add https://github.com/boost-ext/di externals/boost-di
add_subdirectory(externals/boost-di)
add_library(Boost::Boost.DI ALIAS Boost.DI)

# https://github.com/hunter-packages/Microsoft.GSL
# git submodule add https://github.com/hunter-packages/Microsoft.GSL externals/gsl
add_subdirectory(externals/gsl)

if(EXISTS "${Protobuf_PROTOC_EXECUTABLE}")
set_target_properties(protobuf::protoc PROPERTIES
  IMPORTED_LOCATION_RELEASE "${Protobuf_PROTOC_EXECUTABLE}")
endif()

# https://github.com/soramitsu/libp2p-sqlite-modern-cpp/tree/hunter
# hunter_add_package(SQLiteModernCpp)
# find_package(SQLiteModernCpp CONFIG REQUIRED)

include(cmake/install.cmake)
include(cmake/libp2p_add_library.cmake)
include(cmake/functions.cmake)
include(${LIBP2P_ROOT}/cmake/san.cmake)

# set(TESTING OFF)
# set(EXAMPLES "Build examples" OFF)
option(CLANG_FORMAT "Enable clang-format target" ON)
option(CLANG_TIDY "Enable clang-tidy checks during compilation" OFF)
option(COVERAGE "Enable generation of coverage info" OFF)
# sanitizers will be enabled only for libp2p, and will be disabled for dependencies
option(ASAN "Enable address sanitizer" OFF)
option(LSAN "Enable leak sanitizer" OFF)
option(MSAN "Enable memory sanitizer" OFF)
option(TSAN "Enable thread sanitizer" OFF)
option(UBSAN "Enable UB sanitizer" OFF)
option(EXPOSE_MOCKS "Make mocks header files visible for child projects" OFF)


## setup compilation flags
if ("${CMAKE_CXX_COMPILER_ID}" MATCHES "^(AppleClang|Clang|GNU)$")
  # enable those flags
  add_flag(-Wall)
  add_flag(-Wextra)
  add_flag(-Woverloaded-virtual)     # warn if you overload (not override) a virtual function
  add_flag(-Wformat=2)               # warn on security issues around functions that format output (ie printf)
  add_flag(-Wmisleading-indentation) # (only in GCC >= 6.0) warn if indentation implies blocks where blocks do not exist
  add_flag(-Wduplicated-cond)        # (only in GCC >= 6.0) warn if if / else chain has duplicated conditions
  add_flag(-Wduplicated-branches)    # (only in GCC >= 7.0) warn if if / else branches have duplicated code
  add_flag(-Wnull-dereference)       # (only in GCC >= 6.0) warn if a null dereference is detected
  add_flag(-Wdouble-promotion)       # (GCC >= 4.6, Clang >= 3.8) warn if float is implicit promoted to double
  add_flag(-Wsign-compare)
  add_flag(-Wtype-limits)            # size_t - size_t >= 0 -> always true

  # disable those flags
  add_flag(-Wno-unused-command-line-argument)    # clang: warning: argument unused during compilation: '--coverage' [-Wunused-command-line-argument]
  add_flag(-Wno-unused-parameter)    # prints too many useless warnings
  add_flag(-Wno-format-nonliteral)   # prints way too many warnings from spdlog
  add_flag(-Wno-gnu-zero-variadic-macro-arguments)   # https://stackoverflow.com/questions/21266380/is-the-gnu-zero-variadic-macro-arguments-safe-to-ignore

  # promote to errors
  add_flag(-Werror-unused-lambda-capture)  # error if lambda capture is unused
  add_flag(-Werror-return-type)      # warning: control reaches end of non-void function [-Wreturn-type]
  add_flag(-Werror-non-virtual-dtor) # warn the user if a class with virtual functions has a non-virtual destructor. This helps catch hard to track down memory errors
  # add_flag(-Werror-sign-compare)     # warn the user if they compare a signed and unsigned numbers
  add_flag(-Werror-reorder)          # field '$1' will be initialized after field '$2'
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
  # using Visual Studio C++
  # TODO(warchant): add flags https://github.com/lefticus/cppbestpractices/blob/master/02-Use_the_Tools_Available.md#msvc
endif ()

if (COVERAGE)
  include(${LIBP2P_ROOT}/cmake/coverage.cmake)
endif ()
if (CLANG_TIDY)
  include(${LIBP2P_ROOT}/cmake/clang-tidy.cmake)
endif ()
if (CLANG_FORMAT)
  include(${LIBP2P_ROOT}/cmake/clang-format.cmake)
endif ()


get_target_property(GSL_INTERFACE_INCLUDE_DIRS GSL INTERFACE_INCLUDE_DIRECTORIES)
get_target_property(spdlog_INCLUDE_DIRS spdlog INCLUDE_DIRECTORIES)

# patch compile error for nullptr_t

file(READ "${LIBP2P_ROOT}/include/libp2p/common/types.hpp" TYPES_HPP)
file(WRITE "${CMAKE_BINARY_DIR}/libp2p/include/libp2p/common/types.hpp"
"#include <memory>\n"
"${TYPES_HPP}"
)
set(PRJ_PATCHED_INCLUDE_DIR "${CMAKE_BINARY_DIR}/libp2p/include")

include_directories(
  INTERFACE
    # project includes
    ${GSL_INTERFACE_INCLUDE_DIRS}
)

include_directories(
    SYSTEM
    # project includes
    ${PRJ_PATCHED_INCLUDE_DIR}
    ${LIBP2P_ROOT}/include
    ${Boost_INCLUDE_DIRS}
    ${spdlog_INCLUDE_DIRS}
    ${OPENSSL_INCLUDE_DIR}
)

add_subdirectory(${LIBP2P_SRC_ROOT}/basic)
add_subdirectory(${LIBP2P_SRC_ROOT}/common)
add_subdirectory(${LIBP2P_SRC_ROOT}/crypto)
add_subdirectory(${LIBP2P_SRC_ROOT}/host)
add_subdirectory(${LIBP2P_SRC_ROOT}/multi)
add_subdirectory(${LIBP2P_SRC_ROOT}/muxer)
add_subdirectory(${LIBP2P_SRC_ROOT}/network)
add_subdirectory(${LIBP2P_SRC_ROOT}/peer)
add_subdirectory(${LIBP2P_SRC_ROOT}/protocol)
add_subdirectory(${LIBP2P_SRC_ROOT}/protocol_muxer)
add_subdirectory(${LIBP2P_SRC_ROOT}/security)
# add_subdirectory(${LIBP2P_SRC_ROOT}/storage)
add_subdirectory(${LIBP2P_SRC_ROOT}/transport)

add_library(p2p
    ${LIBP2P_SRC_ROOT}/libp2p.cpp
)

target_link_libraries(p2p INTERFACE
    Boost::Boost.DI
    p2p_basic_host
    p2p_default_host
    p2p_default_network
    p2p_peer_repository
    p2p_inmem_address_repository
    p2p_inmem_key_repository
    p2p_inmem_protocol_repository
    p2p_protocol_echo
    p2p_literals
    GSL
)

#libp2p_install(p2p)

