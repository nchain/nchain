add_subdirectory( crypto )
add_subdirectory( persistence )
add_subdirectory( softfloat )

target_compile_options(softfloat PRIVATE -Wno-unused-label )

## appbase

add_subdirectory( appbase )

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

## entities
file( GLOB ENTITIES_HEADERS "entities/*.h" "entities/*.hpp" )

set(ENTITIES_SOURCES
    entities/account.cpp
    entities/cdp.cpp
    entities/contract.cpp
    entities/dexorder.cpp
    entities/id.cpp
    entities/key.cpp
    entities/keystore.cpp
    entities/proposal.cpp
)

set(ENTITIES_INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/entities)

## chain
file( GLOB CHAIN_HEADERS "chain/*.h" "chain/*.hpp" )

set(CHAIN_SOURCES
    chain/blockdelegates.cpp
    chain/chain.cpp
    chain/merkletree.cpp
    chain/sigcache.cpp
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
file( GLOB_RECURSE PERSISTENCE_HEADERS "persistence/*.h" "persistence/*.hpp" )

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

## tx
file( GLOB WALLET_HEADERS "wallet/*.h" "wallet/*.hpp" )

set(WALLET_SOURCES
    wallet/crypter.cpp
    wallet/db.cpp
    wallet/wallet.cpp
    wallet/walletdb.cpp
)

set(WALLET_INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/tx)

## WASM
file( GLOB WASM_HEADERS "wasm/*.h" "wasm/*.hpp" )

set(WASM_SOURCES
    wasm/abi_serializer.cpp
    wasm/wasm_context.cpp
    wasm/wasm_context_rpc.cpp
    wasm/wasm_control_rpc.cpp
    wasm/exception/exception.cpp
    wasm/exception/log_message.cpp
    wasm/wasm_interface.cpp
    wasm/wasm_runtime.cpp
    wasm/types/uint128.cpp
    wasm/types/time.cpp
    wasm/compiler_builtins/fixdfti.c
    wasm/compiler_builtins/fixsfti.c
    wasm/compiler_builtins/fixtfti.c
    wasm/compiler_builtins/fixunsdfti.c
    wasm/compiler_builtins/fixunssfti.c
    wasm/compiler_builtins/fixunstfti.c
    wasm/compiler_builtins/floattidf.c
    wasm/compiler_builtins/floatuntidf.c
)

set (WASM_INCLUDES
    ${CMAKE_CURRENT_SOURCE_DIR}/wasm
    ${CMAKE_CURRENT_SOURCE_DIR}/wasm/types
    ${CMAKE_CURRENT_SOURCE_DIR}/wasm/eos-vm/include
    ${CMAKE_CURRENT_SOURCE_DIR}/wasm/compiler_builtins
    # ${CMAKE_CURRENT_SOURCE_DIR}/wasm/external/outcome/single-header
)

## tx
file( GLOB TX_HEADERS "tx/*.h" "tx/*.hpp" )

set(TX_SOURCES
    tx/accountpermscleartx.cpp
    tx/accountregtx.cpp
    tx/assettx.cpp
    tx/blockpricemediantx.cpp
    tx/blockrewardtx.cpp
    tx/cdptx.cpp
    tx/coinminttx.cpp
    tx/coinstaketx.cpp
    tx/cointransfertx.cpp
    tx/coinutxotx.cpp
    # tx/contracttx.cpp
    tx/delegatetx.cpp
    tx/dexoperatortx.cpp
    tx/dextx.cpp
    tx/pricefeedtx.cpp
    tx/proposaltx.cpp
    tx/tx.cpp
    tx/txmempool.cpp
    tx/universaltx.cpp
)

set(TX_INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/tx)

## miner
file( GLOB MINER_HEADERS "miner/*.h" "miner/*.hpp" )

set(MINER_SOURCES
    miner/miner.cpp
    miner/pbftcontext.cpp
    miner/pbftmanager.cpp
)

set(TX_INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/tx)

## rpc
file( GLOB RPC_HEADERS "rpc/*.h" "rpc/*.hpp" )

set(RPC_SOURCES
    rpc/core/httpserver.cpp
    rpc/core/rpcclient.cpp
    rpc/core/rpccommons.cpp
    rpc/core/rpcprotocol.cpp
    rpc/core/rpcserver.cpp
    rpc/rpcblockchain.cpp
    rpc/rpcdex.cpp
    rpc/rpcdump.cpp
    rpc/rpcmining.cpp
    rpc/rpcmisc.cpp
    rpc/rpcnet.cpp
    rpc/rpcproposal.cpp
    rpc/rpcscoin.cpp
    rpc/rpcsubmittx.cpp
    rpc/rpctpstester.cpp
    rpc/rpctx.cpp
    rpc/rpctxserializer.cpp
    rpc/rpcutxo.cpp
    rpc/rpcwallet.cpp
)

set(RPC_INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/rpc ${CMAKE_CURRENT_SOURCE_DIR}/rpc/core)

set(HEADERS ${CONFIG_HEADERS} ${COMMONS_HEADERS} ${CRYPTO_HEADERS} ${ENTITIES_HEADERS} ${P2P_HEADERS} ${CHAIN_HEADERS}
    ${PERSISTENCE_HEADERS} ${WALLET_HEADERS} ${WASM_HEADERS} ${TX_HEADERS} ${MINER_HEADERS} ${RPC_HEADERS} )
set(SOURCES ${CONFIG_SOURCES} ${COMMONS_SOURCES} ${CRYPTO_SOURCES} ${ENTITIES_SOURCES} ${P2P_SOURCES} ${CHAIN_SOURCES}
    ${PERSISTENCE_SOURCES} ${WALLET_SOURCES} ${WASM_SOURCES} ${TX_SOURCES} ${MINER_SOURCES} ${RPC_SOURCES} )

add_library( commons STATIC
             ${SOURCES}
             ${HEADERS} )

target_link_libraries( commons
    PRIVATE
       secp256k1 leveldb softfloat
    PUBLIC
        BerkeleyDB::CXX OpenSSL::Crypto
)

# set_target_properties(commons PROPERTIES CXX_VISIBILITY_PRESET hidden)
#target_compile_options(commons PRIVATE -fPIC )

target_include_directories( commons
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/commons
        ${CMAKE_CURRENT_SOURCE_DIR}/config
        ${ENTITIES_INCLUDES}
        ${CMAKE_CURRENT_SOURCE_DIR}/chain
        ${CMAKE_CURRENT_SOURCE_DIR}/p2p
        ${CMAKE_CURRENT_SOURCE_DIR}/persistence
        ${CMAKE_CURRENT_SOURCE_DIR}/persistence/leveldb/helpers/memenv
        ${WALLET_INCLUDES}
        ${WASM_INCLUDES}
        ${TX_INCLUDES}
        ${MINER_INCLUDES}
        ${RPC_INCLUDES}
    PRIVATE
        ${Boost_INCLUDE_DIRS}
)
