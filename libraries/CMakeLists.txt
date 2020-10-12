add_subdirectory( crypto )
add_subdirectory( persistence )

## commons

file( GLOB_RECURSE COMMONS_HEADERS "commons/*.h" "commons/*.hpp"
)

file( GLOB CRYPTO_HEADERS "crypto/*.h" "crypto/*.hpp" )

set(COMMONS_SOURCES
    commons/allocators.cpp
    commons/arith_uint256.cpp
    commons/base58.cpp
    commons/bloom.cpp
    commons/fs.cpp
    commons/logging.cpp
    commons/random.cpp
    commons/uint256.cpp
    commons/sync.cpp
    # commons/compat/glibc_compat.cpp
    # commons/compat/glibcxx_compat.cpp
    commons/support/cleanse.cpp
    commons/support/events.cpp
    commons/util/threadnames.cpp
    commons/util/time.cpp
    commons/util/util.cpp
    commons/json/json_spirit_reader.cpp
    commons/json/json_spirit_value.cpp
    commons/json/json_spirit_writer.cpp
    crypto/aes.cpp
    crypto/chacha20.cpp
    crypto/hash.cpp
    crypto/hmac_sha256.cpp
    crypto/hmac_sha512.cpp
    crypto/ripemd160.cpp
    crypto/sha1.cpp
    crypto/sha256.cpp
    crypto/sha256_avx2.cpp
    crypto/sha256_shani.cpp
    crypto/sha256_sse4.cpp
    crypto/sha256_sse41.cpp
    crypto/sha512.cpp
    # crypto/siphash.cpp
)

## config
file( GLOB CONFIG_HEADERS "commons/*.h" "commons/*.hpp" )

set(CONFIG_SOURCES
    config/chainparams.cpp
    config/configuration.cpp
    config/version.cpp
)

## crypto

file( GLOB CRYPTO_HEADERS "crypto/*.h" "crypto/*.hpp" )

set(CRYPTO_SOURCES
    crypto/aes.cpp
    crypto/chacha20.cpp
    crypto/hash.cpp
    crypto/hmac_sha256.cpp
    crypto/hmac_sha512.cpp
    crypto/ripemd160.cpp
    crypto/sha1.cpp
    crypto/sha256.cpp
    crypto/sha256_avx2.cpp
    crypto/sha256_shani.cpp
    crypto/sha256_sse4.cpp
    crypto/sha256_sse41.cpp
    crypto/sha512.cpp
    # crypto/siphash.cpp
)

## chain
file( GLOB CHAIN_HEADERS "chain/*.h" "chain/*.hpp" )

set(CHAIN_SOURCES
    chain/blockdelegates.cpp
    chain/chain.cpp
    chain/merkletree.cpp
    chain/validation.cpp
)

## p2p
file( GLOB P2P_HEADERS "p2p/*.h" "p2p/*.hpp" )

set(P2P_SOURCES
    p2p/addrman.cpp
    p2p/alert.cpp
    p2p/chainmessage.cpp
    p2p/net.cpp
    p2p/netbase.cpp
    p2p/netmessage.cpp
    p2p/node.cpp
    p2p/protocol.cpp
)


## persistence
file( GLOB PERSISTENCE_HEADERS "persistence/*.h" "persistence/*.hpp" )

set(PERSISTENCE_SOURCES
    persistence/accountdb.cpp
    persistence/assetdb.cpp
    persistence/block.cpp
    persistence/blockdb.cpp
    persistence/blockundo.cpp
    persistence/cachewrapper.cpp
    persistence/cdpdb.cpp
    persistence/contractdb.cpp
    persistence/delegatedb.cpp
    persistence/dexdb.cpp
    persistence/disk.cpp
    persistence/leveldbwrapper.cpp
    persistence/logdb.cpp
    persistence/pricefeeddb.cpp
    persistence/txdb.cpp
    persistence/txreceiptdb.cpp
    persistence/txutxodb.cpp
)

set(HEADERS ${CONFIG_HEADERS} ${COMMONS_HEADERS} ${CRYPTO_HEADERS} ${P2P_HEADERS} ${CHAIN_HEADERS} ${PERSISTENCE_HEADERS} )
set(SOURCES ${CONFIG_SOURCES} ${COMMONS_SOURCES} ${CRYPTO_SOURCES} ${P2P_SOURCES} ${CHAIN_SOURCES} ${PERSISTENCE_SOURCES} )

add_library( commons STATIC
             ${SOURCES}
             ${HEADERS} )

target_link_libraries( commons
    PRIVATE
        Boost::filesystem secp256k1 leveldb  Oracle::BerkeleyDB
)

#target_link_libraries( commons Boost::filesystem Threads::Threads )
target_include_directories( commons
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/commons
        ${CMAKE_CURRENT_SOURCE_DIR}/config
        ${CMAKE_CURRENT_SOURCE_DIR}/entities
        ${CMAKE_CURRENT_SOURCE_DIR}/chain
        ${CMAKE_CURRENT_SOURCE_DIR}/p2p
        ${CMAKE_CURRENT_SOURCE_DIR}/persistence
        ${CMAKE_CURRENT_SOURCE_DIR}/persistence/leveldb/helpers/memenv
)
