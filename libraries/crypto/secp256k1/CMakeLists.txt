cmake_minimum_required(VERSION 3.4)
project(secp256k1)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/CMakeModules")
if (UNIX AND APPLE)
   #list(APPEND CMAKE_PREFIX_PATH "/usr/local/opt/gettext")
endif()

file(GLOB HEADERS "src/*.h")

find_package(GMP REQUIRED)

add_library(secp256k1 STATIC
    src/secp256k1.c
    ${HEADERS}
)

target_include_directories(secp256k1
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${GMP_INCLUDE_DIR}
)

target_compile_definitions(secp256k1 PRIVATE HAVE_CONFIG_H=1)

target_link_libraries(secp256k1 ${GMP_LIBRARIES})

install( TARGETS secp256k1
   RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
   LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
   ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
)
