// Copyright (c) 2009-2010 Satoshi Nakamoto
// Copyright (c) 2020-2021 The nchain Developers
// Distributed under the MIT/X11 software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#ifndef CHAIN_VALIDATION_H
#define CHAIN_VALIDATION_H


#include <string>

#include "commons/json/json_spirit.h"

/** Capture information about block/transaction validation */
class CValidationState {
private:
    enum mode_state {
        MODE_VALID,   // everything ok
        MODE_INVALID, // network rule violation (DoS value may be set)
        MODE_ERROR,   // run-time error
    };
    mode_state mode = MODE_VALID;
    int32_t nDoS    = 0;
    std::string rejectReason;
    uint8_t rejectCode      = 0;
    bool corruptionPossible = false;

    std::string ret;

    std::optional<json_spirit::Value> opt_trace;

public:
    CValidationState() {}
    CValidationState(bool isTrace) {
        if (isTrace)
            opt_trace = json_spirit::Value();
    }

    bool DoS(int32_t level, bool ret = false, uint8_t rejectCodeIn = 0, std::string rejectReasonIn = "",
             bool corruptionIn = false);
    bool Invalid(bool ret = false, uint8_t rejectCodeIn = 0, std::string rejectReasonIn = "");
    bool Error(std::string rejectReasonIn = "");
    bool Abort(const std::string &msg);
    bool IsValid() const;
    bool IsInvalid() const;
    bool IsError() const;
    bool IsInvalid(int32_t &nDoSOut) const;
    bool CorruptionPossible() const;
    uint8_t GetRejectCode() const;
    std::string GetRejectReason() const;

    void SetReturn(std::string r);
    std::string GetReturn();

    const std::optional<json_spirit::Value> &GetTrace() const;
    std::optional<json_spirit::Value> &GetTrace();
};

#endif//CHAIN_VALIDATION_H