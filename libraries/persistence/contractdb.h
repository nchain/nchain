// Copyright (c) 2009-2010 Satoshi Nakamoto
// Copyright (c) 2020-2021 The nchain Developers
// Distributed under the MIT/X11 software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#ifndef PERSIST_CONTRACTDB_H
#define PERSIST_CONTRACTDB_H

#include "entities/account.h"
#include "entities/contract.h"
#include "entities/key.h"
#include "commons/arith_uint256.h"
#include "commons/uint256.h"
#include "dbcache.h"
#include "dbiterator.h"
#include "persistence/disk.h"

#include <map>
#include <string>
#include <utility>
#include <vector>

class CKeyID;
class CRegID;
class CAccount;
class CContractDB;
class CCacheWrapper;

struct CDiskTxPos;

typedef dbk::CDBTailKey<MAX_CONTRACT_KEY_SIZE> CDBContractKey;

/*  CCompositeKVCache     prefixType                       key                       value         variable           */
/*  -------------------- --------------------         ----------------------------  ---------   --------------------- */
    // pair<contractRegId, contractKey> -> contractData
typedef CCompositeKVCache< dbk::CONTRACT_DATA,        pair<CRegIDKey, CDBContractKey>, string>     DBContractDataCache;

class CDBContractDataIterator: public CDBPrefixIterator<DBContractDataCache, DBContractDataCache::KeyType> {
private:
    typedef typename DBContractDataCache::KeyType KeyType;
    typedef CDBPrefixIterator<DBContractDataCache, DBContractDataCache::KeyType> Base;
public:
    CDBContractDataIterator(DBContractDataCache &dbCache, const CRegID &regidIn,
                            const string &contractKeyPrefix)
        : Base(dbCache, KeyType(CRegIDKey(regidIn), contractKeyPrefix)) {}

    bool SeekUpper(const string *pLastContractKey) {
        if (pLastContractKey == nullptr || db_util::IsEmpty(*pLastContractKey))
            return First();
        if (pLastContractKey->size() > CDBContractKey::MAX_KEY_SIZE)
            return false;
        KeyType lastKey(GetPrefixElement().first, *pLastContractKey);
        return sp_it_Impl->SeekUpper(&lastKey);
    }

    const string& GetContractKey() const {
        return GetKey().second.GetKey();
    }
};

class CContractDBCache {
public:
    CContractDBCache() {}

    CContractDBCache(CDBAccess *pDbAccess):
        contractCache(pDbAccess),
        contractDataCache(pDbAccess),
        contractTracesCache(pDbAccess),
        contractLogsCache(pDbAccess) {
        assert(pDbAccess->GetDbNameType() == DBNameType::CONTRACT);
    };

    CContractDBCache(CContractDBCache *pBaseIn):
        contractCache(pBaseIn->contractCache),
        contractDataCache(pBaseIn->contractDataCache),
        contractTracesCache(pBaseIn->contractTracesCache),
        contractLogsCache(pBaseIn->contractLogsCache) {};

    bool GetContract(const CRegID &contractRegId, CUniversalContractStore &contractStore);
    bool SaveContract(const CRegID &contractRegId, const CUniversalContractStore &contractStore);
    bool HasContract(const CRegID &contractRegId);
    bool EraseContract(const CRegID &contractRegId);

    bool GetContractData(const CRegID &contractRegId, const string &contractKey, string &contractData);
    bool SetContractData(const CRegID &contractRegId, const string &contractKey, const string &contractData);
    bool HasContractData(const CRegID &contractRegId, const string &contractKey);
    bool EraseContractData(const CRegID &contractRegId, const string &contractKey);

    bool GetContractTraces(const uint256 &txid, string &contractTraces);
    bool SetContractTraces(const uint256 &txid, const string &contractTraces);

    bool GetContractLogs(const uint256 &txid, string &contractLogs);
    bool SetContractLogs(const uint256 &txid, const string &contractLogs);

    bool Flush();
    uint32_t GetCacheSize() const;

    void SetBaseViewPtr(CContractDBCache *pBaseIn) {
        contractCache.SetBase(&pBaseIn->contractCache);
        contractDataCache.SetBase(&pBaseIn->contractDataCache);
        contractTracesCache.SetBase(&pBaseIn->contractTracesCache);
        contractLogsCache.SetBase(&pBaseIn->contractLogsCache);
    };

    void SetDbOpLogMap(CDBOpLogMap *pDbOpLogMapIn) {
        contractCache.SetDbOpLogMap(pDbOpLogMapIn);
        contractDataCache.SetDbOpLogMap(pDbOpLogMapIn);
        contractTracesCache.SetDbOpLogMap(pDbOpLogMapIn);
        contractLogsCache.SetDbOpLogMap(pDbOpLogMapIn);
    }

    void RegisterUndoFunc(UndoDataFuncMap &undoDataFuncMap) {
        contractCache.RegisterUndoFunc(undoDataFuncMap);
        contractDataCache.RegisterUndoFunc(undoDataFuncMap);
        contractTracesCache.RegisterUndoFunc(undoDataFuncMap);
        contractLogsCache.RegisterUndoFunc(undoDataFuncMap);
    }

    shared_ptr<CDBContractDataIterator> CreateContractDataIterator(const CRegID &contractRegid,
        const string &contractKeyPrefix);

public:
/*       type               prefixType               key                     value                 variable               */
/*  ----------------   -------------------------   -----------------------  ------------------   ------------------------ */
    /////////// ContractDB
    // contract $RegIdKey -> CUniversalContractStore
    CCompositeKVCache< dbk::CONTRACT_DEF,         CRegIDKey,                  CUniversalContractStore >   contractCache;

    // pair<contractRegId, contractKey> -> contractData
    DBContractDataCache contractDataCache;

    // txid -> contract_traces
    CCompositeKVCache< dbk::CONTRACT_TRACES,     uint256,                      string >                    contractTracesCache;
    CCompositeKVCache< dbk::CONTRACT_LOGS,       uint256,                      string >                    contractLogsCache;
};

#endif  // PERSIST_CONTRACTDB_H