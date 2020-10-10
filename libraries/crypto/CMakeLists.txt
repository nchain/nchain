
# file(GLOB HEADERS "*.h" "*.hpp")
# set(CRYPTO_SOURCES
#     aes.cpp
#     chacha20.cpp
#     hash.cpp
#     hmac_sha256.cpp
#     hmac_sha512.cpp
#     ripemd160.cpp
#     sha1.cpp
#     sha256.cpp
#     sha256_avx2.cpp
#     sha256_shani.cpp
#     sha256_sse4.cpp
#     sha256_sse41.cpp
#     sha512.cpp
#     siphash.cpp
# )

# add_library( chain_crypto
#              ${CRYPTO_SOURCES}
#              ${HEADERS} )

# target_link_libraries( chain_crypto Boost::thread )

# target_include_directories( chain_crypto
#     PUBLIC
#         ${CMAKE_CURRENT_SOURCE_DIR}
#         ${CMAKE_CURRENT_SOURCE_DIR}/..
# )

add_subdirectory( secp256k1 )