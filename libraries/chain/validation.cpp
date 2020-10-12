// Copyright (c) 2009-2010 Satoshi Nakamoto
// Copyright (c) 2020-2021 The nchain Developers
// Distributed under the MIT/X11 software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#include "validation.h"

using namespace std;

extern bool AbortNode(const string &strMessage);

/** Capture information about block/transaction validation */
// class CValidationState

bool CValidationState::DoS(int32_t level, bool ret, uint8_t rejectCodeIn,
                           string rejectReasonIn, bool corruptionIn) {
    rejectCode         = rejectCodeIn;
    rejectReason       = rejectReasonIn;
    corruptionPossible = corruptionIn;
    if (mode == MODE_ERROR)
        return ret;
    nDoS += level;
    mode = MODE_INVALID;
    return ret;
}

bool CValidationState::Invalid(bool ret, uint8_t rejectCodeIn,
                               string rejectReasonIn) {
    return DoS(0, ret, rejectCodeIn, rejectReasonIn);
}
bool CValidationState::Error(string rejectReasonIn) {
    if (mode == MODE_VALID)
        rejectReason = rejectReasonIn;
    mode = MODE_ERROR;
    return false;
}

bool CValidationState::Abort(const string &msg) {
    AbortNode(msg);
    return Error(msg);
}
bool CValidationState::IsValid() const { return mode == MODE_VALID; }

bool CValidationState::IsInvalid() const { return mode == MODE_INVALID; }

bool CValidationState::IsError() const { return mode == MODE_ERROR; }

bool CValidationState::IsInvalid(int32_t &nDoSOut) const {
    if (IsInvalid()) {
        nDoSOut = nDoS;
        return true;
    }
    return false;
}

bool CValidationState::CorruptionPossible() const { return corruptionPossible; }

uint8_t CValidationState::GetRejectCode() const { return rejectCode; }

string CValidationState::GetRejectReason() const { return rejectReason; }

void CValidationState::SetReturn(std::string r) { ret = r; }

std::string CValidationState::GetReturn() { return ret; }

const std::optional<json_spirit::Value> &CValidationState::GetTrace() const { return opt_trace; }

std::optional<json_spirit::Value> &CValidationState::GetTrace() { return opt_trace; }
