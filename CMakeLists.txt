cmake_minimum_required( VERSION 3.8 )

project( NCHAIN )
include(CTest) # suppresses DartConfiguration.tcl error
enable_testing()

include(CheckIncludeFiles)
include(CheckSymbolExists)

if( APPLE AND UNIX )
# Apple MacOS
## fix the link warning "... different visibility settings"
## @see https://stackoverflow.com/questions/9894961/strange-warnings-from-the-linker-ld
## @ see https://stackoverflow.com/questions/17080869/what-is-the-cmake-equivalent-to-gcc-fvisibility-hidden-when-controlling-the-e
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
endif()

if ("${CMAKE_INSTALL_PREFIX}" STREQUAL "/usr/local")
   message(WARNING "CMAKE_INSTALL_PREFIX is set to /usr/local. This is not recommended.")
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/CMakeModules")

# if (UNIX AND APPLE)
#    list(APPEND CMAKE_PREFIX_PATH "/usr/local/opt/gettext")
# endif()

include( GNUInstallDirs )
# include( InstallDirectoryPermissions )
# include( MASSigning )

set( BLOCKCHAIN_NAME "NCHAIN" )
set( CMAKE_CXX_STANDARD 17 )
set( CMAKE_CXX_EXTENSIONS ON )
set( CXX_STANDARD_REQUIRED ON)

# version MAJOR.MINOR.PATCH
set(VERSION_MAJOR 0)
set(VERSION_MINOR 1)
set(VERSION_PATCH 1)
#set(VERSION_SUFFIX rc3)

if(VERSION_SUFFIX)
    set(VERSION_FULL "${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}-${VERSION_SUFFIX}")
else()
    set(VERSION_FULL "${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}")
endif()

set( CLI_CLIENT_EXECUTABLE_NAME ncc )
set( NODE_EXECUTABLE_NAME ncd )

# http://stackoverflow.com/a/18369825
if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 7.0)
        message(FATAL_ERROR "GCC version must be at least 7.0!")
    endif()
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang" OR "${CMAKE_CXX_COMPILER_ID}" STREQUAL "AppleClang")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 5.0)
        message(FATAL_ERROR "Clang version must be at least 5.0!")
    endif()
endif()

if ("${CMAKE_GENERATOR}" STREQUAL "Ninja")
   add_compile_options(-fdiagnostics-color=always)
endif()

# Endianness
CHECK_INCLUDE_FILES("endian.h" HAVE_ENDIAN_H)
check_include_files("sys/endian.h" HAVE_SYS_ENDIAN_H)

if(HAVE_ENDIAN_H)
	set(ENDIAN_FILE "endian.h")
elseif(HAVE_SYS_ENDIAN_H)
	set(ENDIAN_FILE "sys/endian.h")
else()
endif()

if(ENDIAN_FILE)
	check_symbol_exists(htole16 ${ENDIAN_FILE} HAVE_DECL_HTOLE16)
	check_symbol_exists(htobe16 ${ENDIAN_FILE} HAVE_DECL_HTOBE16)
	check_symbol_exists(be16toh ${ENDIAN_FILE} HAVE_DECL_BE16TOH)
	check_symbol_exists(le16toh ${ENDIAN_FILE} HAVE_DECL_LE16TOH)
	check_symbol_exists(htobe32 ${ENDIAN_FILE} HAVE_DECL_HTOBE32)
	check_symbol_exists(htole32 ${ENDIAN_FILE} HAVE_DECL_HTOLE32)
	check_symbol_exists(be32toh ${ENDIAN_FILE} HAVE_DECL_BE32TOH)
	check_symbol_exists(le32toh ${ENDIAN_FILE} HAVE_DECL_LE32TOH)
	check_symbol_exists(htobe64 ${ENDIAN_FILE} HAVE_DECL_HTOBE64)
	check_symbol_exists(htole64 ${ENDIAN_FILE} HAVE_DECL_HTOLE64)
	check_symbol_exists(be64toh ${ENDIAN_FILE} HAVE_DECL_BE64TOH)
	check_symbol_exists(le64toh ${ENDIAN_FILE} HAVE_DECL_LE64TOH)
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS "ON")
set(BUILD_DOXYGEN FALSE CACHE BOOL "Build doxygen documentation on every make")
# set(BUILD_MONGO_DB_PLUGIN FALSE CACHE BOOL "Build mongo database plugin")
# set(ENABLE_MULTIVERSION_PROTOCOL_TEST FALSE CACHE BOOL "Enable nodeos multiversion protocol test")

IF(NOT "$ENV{OPENSSL_ROOT_DIR}" STREQUAL "")
  set(OPENSSL_ROOT_DIR $ENV{OPENSSL_ROOT_DIR} )
  set(OPENSSL_INCLUDE_DIR ${OPENSSL_ROOT_DIR}/include)
  message(STATUS "Setting up OpenSSL root and include vars to ${OPENSSL_ROOT_DIR}, ${OPENSSL_INCLUDE_DIR}")
ENDIF()

# add defaults for openssl
if(APPLE AND UNIX AND "${OPENSSL_ROOT_DIR}" STREQUAL "")
   set(OPENSSL_ROOT_DIR "/usr/local/opt/openssl@1.1")
endif()

find_package(OpenSSL REQUIRED)

# WASM runtimes to enable. Each runtime in this list will have:
#  * definition NCHAIN_<RUNTIME>_RUNTIME_ENABLED defined in public libchain interface
#  * ctest entries with --runtime
# list(APPEND NCHAIN_WASM_RUNTIMES wabt)   #always enable wabt; it works everywhere and parts of nchain still assume it's always available
# if(CMAKE_SIZEOF_VOID_P EQUAL 8 AND NOT WIN32)
#    if("${CMAKE_SYSTEM_NAME}" STREQUAL "Linux" AND "${CMAKE_SYSTEM_PROCESSOR}" STREQUAL "x86_64")
#       list(APPEND NCHAIN_WASM_RUNTIMES nchain-vm-oc)
#       # NCHAIN VM OC requires LLVM, but move the check up here to a central location so that the NchainTester.cmakes
#       # can be created with the exact version found
#       find_package(LLVM REQUIRED CONFIG)
#       if(LLVM_VERSION_MAJOR VERSION_LESS 7 OR LLVM_VERSION_MAJOR VERSION_GREATER 9)
# 	      message(FATAL_ERROR "NCHAIN requires an LLVM version 7.0 to 9.0")
#       endif()
#    endif()
# endif()

if(CMAKE_SIZEOF_VOID_P EQUAL 8 AND NOT WIN32)
   if(CMAKE_SYSTEM_PROCESSOR STREQUAL x86_64)
      list(APPEND NCHAIN_WASM_RUNTIMES nchain-vm nchain-vm-jit)
   endif()
endif()

if(UNIX)
  if(APPLE)
    set(whole_archive_flag "-force_load")
    set(no_whole_archive_flag "")
  else()
    set(whole_archive_flag "--whole-archive")
    set(no_whole_archive_flag "--no-whole-archive")
  endif()
else()
  set(whole_archive_flag "--whole-archive")
  set(no_whole_archive_flag "--no-whole-archive")
endif()

set( Boost_USE_STATIC_LIBS ON CACHE STRING "ON or OFF" )
# Most boost deps get implictly picked up via fc, as just about everything links to fc. In addition we pick up
# the pthread dependency through fc.
find_package(Boost 1.67 REQUIRED COMPONENTS thread system program_options filesystem unit_test_framework)

find_package(BerkeleyDB REQUIRED)

find_package(Event 2.0.22 REQUIRED COMPONENTS event pthreads)

# set( CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC -pie" )
# set(CMAKE_POSITION_INDEPENDENT_CODE ON)
if( APPLE AND UNIX )
# Apple Specific Options Here
    message( STATUS "Configuring NCHAIN on macOS" )
    set( CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_CXX_FLAGS} -Wall -Wno-deprecated-declarations -DMAC_OS -isystem /usr/local/include" )
else()
    # Linux Specific Options Here
    message( STATUS "Configuring NCHAIN on Linux" )
    set( CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_CXX_FLAGS} -Wall" )
    if ( FULL_STATIC_BUILD )
      set( CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libstdc++ -static-libgcc")
    endif ( FULL_STATIC_BUILD )

    if( "${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang" )
        if( CMAKE_CXX_COMPILER_VERSION VERSION_EQUAL 4.0.0 OR CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 4.0.0 )
            set( CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-invalid-partial-specialization" )
        endif()
    endif()
endif()

# disable inconsistent-missing-override
set( CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-inconsistent-missing-override -Wno-overloaded-virtual -Wno-unused-variable -Wno-unused-function -Wno-range-loop-analysis")

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang" OR "${CMAKE_CXX_COMPILER_ID}" STREQUAL "AppleClang")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL 12.0)
      set( CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-tautological-bitwise-compare")
    endif()
endif()

## TODO: must remove NDEBUG for assert
# set( CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DNDEBUG" )

# based on http://www.delorie.com/gnu/docs/gdb/gdb_70.html
# uncomment this line to tell GDB about macros (slows compile times)
# set( CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -gdwarf-2 -g3" )

set(ENABLE_COVERAGE_TESTING FALSE CACHE BOOL "Build NCHAIN for code coverage analysis")

if(ENABLE_COVERAGE_TESTING)
  SET(CMAKE_CXX_FLAGS "--coverage ${CMAKE_CXX_FLAGS}")
  find_program( LCOV_PATH lcov )
  find_program( LLVMCOV_PATH  llvm-cov )
  find_program( GENHTML_PATH NAMES genhtml)
endif()

# include(utils)

# if ("${CORE_SYMBOL_NAME}" STREQUAL "")
#   set( CORE_SYMBOL_NAME "SYS" )
# endif()
# string(TOUPPER ${CORE_SYMBOL_NAME} CORE_SYMBOL_NAME)

# string(LENGTH ${CORE_SYMBOL_NAME} CORE_SYMBOL_NAME_LENGTH)
# if (CORE_SYMBOL_NAME_LENGTH GREATER 7)
#   message(FATAL_ERROR "CORE_SYMBOL_NAME length must be between 1 and 7 characters")
# endif()

# message( STATUS "Using '${CORE_SYMBOL_NAME}' as CORE symbol name" )

if ("${NCHAIN_ROOT_KEY}" STREQUAL "")
   # TODO: change the prefix 'EOS' to 'NCHAIN'
   set(NCHAIN_ROOT_KEY "EOS6MRyAjQq8ud7hVNYcfnVPJqcVpscN5So8BhtHuGYqET5GDW5CV")
endif()

message( STATUS "Using '${NCHAIN_ROOT_KEY}' as public key for 'nchain' account" )

add_subdirectory( libraries )
add_subdirectory( plugins   )
add_subdirectory( programs  )


# add_subdirectory( scripts )
# add_subdirectory( unittests )
# add_subdirectory( tests )
# add_subdirectory( tools )

# if (NOT DISABLE_WASM_SPEC_TESTS)
# add_subdirectory( nchain-wasm-spec-tests/generated-tests )
# endif()

# install_directory_permissions(DIRECTORY ${CMAKE_INSTALL_FULL_SYSCONFDIR}/nchain)

# install(FILES testnet.template DESTINATION ${CMAKE_INSTALL_FULL_SYSCONFDIR}/nchain/launcher)
# install_directory_permissions(DIRECTORY ${CMAKE_INSTALL_FULL_SYSCONFDIR}/nchain)
# install_directory_permissions(DIRECTORY ${CMAKE_INSTALL_FULL_SYSCONFDIR}/nchain/launcher)
# configure_file(${CMAKE_CURRENT_SOURCE_DIR}/testnet.template ${CMAKE_CURRENT_BINARY_DIR}/etc/nchain/launcher/testnet.template COPYONLY)

# configure_file(${CMAKE_CURRENT_SOURCE_DIR}/nchain.version.in ${CMAKE_CURRENT_BINARY_DIR}/nchain.version.hpp)
# install(FILES ${CMAKE_CURRENT_BINARY_DIR}/nchain.version.hpp DESTINATION ${CMAKE_INSTALL_FULL_INCLUDEDIR})

# set(NCHAIN_ROOT_DIR ${CMAKE_BINARY_DIR})
# configure_file(${CMAKE_SOURCE_DIR}/CMakeModules/nchain-config.cmake.in ${CMAKE_BINARY_DIR}/lib/cmake/nchain/nchain-config.cmake @ONLY)
# configure_file(${CMAKE_SOURCE_DIR}/CMakeModules/NchainTesterBuild.cmake.in ${CMAKE_BINARY_DIR}/lib/cmake/nchain/NchainTester.cmake @ONLY)

# set(NCHAIN_ROOT_DIR ${CMAKE_INSTALL_PREFIX})
# configure_file(${CMAKE_SOURCE_DIR}/CMakeModules/nchain-config.cmake.in ${CMAKE_BINARY_DIR}/modules/nchain-config.cmake @ONLY)
# install(FILES ${CMAKE_BINARY_DIR}/modules/nchain-config.cmake DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}/cmake/nchain)
# configure_file(${CMAKE_SOURCE_DIR}/CMakeModules/NchainTester.cmake.in ${CMAKE_BINARY_DIR}/modules/NchainTester.cmake @ONLY)
# install(FILES ${CMAKE_BINARY_DIR}/modules/NchainTester.cmake DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}/cmake/nchain)

# configure_file(${CMAKE_SOURCE_DIR}/LICENSE
#                ${CMAKE_BINARY_DIR}/licenses/nchain/LICENSE COPYONLY)
# configure_file(${CMAKE_SOURCE_DIR}/libraries/wabt/LICENSE
#                ${CMAKE_BINARY_DIR}/licenses/nchain/LICENSE.wabt COPYONLY)
# configure_file(${CMAKE_SOURCE_DIR}/libraries/softfloat/COPYING.txt
#                ${CMAKE_BINARY_DIR}/licenses/nchain/LICENSE.softfloat COPYONLY)
# configure_file(${CMAKE_SOURCE_DIR}/libraries/wasm-jit/LICENSE
#                ${CMAKE_BINARY_DIR}/licenses/nchain/LICENSE.wavm COPYONLY)
# configure_file(${CMAKE_SOURCE_DIR}/libraries/fc/secp256k1/upstream/COPYING
#                ${CMAKE_BINARY_DIR}/licenses/nchain/LICENSE.secp256k1 COPYONLY)
# configure_file(${CMAKE_SOURCE_DIR}/libraries/fc/src/network/LICENSE.go
#                ${CMAKE_BINARY_DIR}/licenses/nchain/LICENSE.go COPYONLY)
# configure_file(${CMAKE_SOURCE_DIR}/libraries/yubihsm/LICENSE
#                ${CMAKE_BINARY_DIR}/licenses/nchain/LICENSE.yubihsm COPYONLY)
# configure_file(${CMAKE_SOURCE_DIR}/libraries/nchain-vm/LICENSE
#                ${CMAKE_BINARY_DIR}/licenses/nchain/LICENSE.nchain-vm COPYONLY)

# install(FILES LICENSE DESTINATION ${CMAKE_INSTALL_FULL_DATAROOTDIR}/licenses/nchain/ COMPONENT base)
# install(FILES libraries/wabt/LICENSE DESTINATION ${CMAKE_INSTALL_FULL_DATAROOTDIR}/licenses/nchain/ RENAME LICENSE.wabt COMPONENT base)
# install(FILES libraries/softfloat/COPYING.txt DESTINATION ${CMAKE_INSTALL_FULL_DATAROOTDIR}/licenses/nchain/ RENAME LICENSE.softfloat COMPONENT base)
# install(FILES libraries/wasm-jit/LICENSE DESTINATION ${CMAKE_INSTALL_FULL_DATAROOTDIR}/licenses/nchain/ RENAME LICENSE.wavm COMPONENT base)
# install(FILES libraries/fc/secp256k1/upstream/COPYING DESTINATION ${CMAKE_INSTALL_FULL_DATAROOTDIR}/licenses/nchain/ RENAME LICENSE.secp256k1 COMPONENT base)
# install(FILES libraries/fc/src/network/LICENSE.go DESTINATION ${CMAKE_INSTALL_FULL_DATAROOTDIR}/licenses/nchain/ COMPONENT base)
# install(FILES libraries/yubihsm/LICENSE DESTINATION ${CMAKE_INSTALL_FULL_DATAROOTDIR}/licenses/nchain/ RENAME LICENSE.yubihsm COMPONENT base)
# install(FILES libraries/nchain-vm/LICENSE DESTINATION ${CMAKE_INSTALL_FULL_DATAROOTDIR}/licenses/nchain/ RENAME LICENSE.nchain-vm COMPONENT base)

# add_custom_target(base-install
#   COMMAND "${CMAKE_COMMAND}" --build "${CMAKE_BINARY_DIR}"
#   COMMAND "${CMAKE_COMMAND}" -DCMAKE_INSTALL_COMPONENT=base -P "${CMAKE_BINARY_DIR}/cmake_install.cmake"
#   USES_TERMINAL
# )

get_property(_CTEST_CUSTOM_TESTS_IGNORE GLOBAL PROPERTY CTEST_CUSTOM_TESTS_IGNORE)
file(WRITE "${CMAKE_BINARY_DIR}/CTestCustom.cmake" "SET(CTEST_CUSTOM_TESTS_IGNORE ${_CTEST_CUSTOM_TESTS_IGNORE})")

# include(package)
# include(doxygen)
