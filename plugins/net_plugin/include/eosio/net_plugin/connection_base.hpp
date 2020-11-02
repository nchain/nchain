// Copyright (c) 2009-2010 Satoshi Nakamoto
// Copyright (c) 2020-2021 The nchain Developers
// Distributed under the MIT/X11 software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#pragma once

#include <eosio/net_plugin/protocol.hpp>

namespace eosio {

struct connection_status {
    std::string       peer;
    bool              connecting = false;
    bool              syncing    = false;
    handshake_message last_handshake;
};

} //namespace eosio
