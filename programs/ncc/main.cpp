// Copyright (c) 2009-2010 Satoshi Nakamoto
// Copyright (c) 2020-2021 The nchain Developers
// Distributed under the MIT/X11 software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

/**
  @defgroup ncclienttool

  @section intro Introduction to ncc

  `ncc` is a command line tool that interfaces with the REST api exposed by @ref nodeos. In order to use `ncc` you will need to
  have a local copy of `nodeos` running and configured to load the 'eosio::chain_api_plugin'.

   ncc contains documentation for all of its commands. For a list of all commands known to ncc, simply run it with no arguments:
```
$ ./ncc
Command Line Interface to EOSIO Client
Usage: programs/ncc/ncc [OPTIONS] SUBCOMMAND

Options:
  -h,--help                   Print this help message and exit
  -u,--url TEXT=http://localhost:8888/
                              the http/https URL where nodeos is running
  --wallet-url TEXT=http://localhost:8888/
                              the http/https URL where keosd is running
  -r,--header                 pass specific HTTP header, repeat this option to pass multiple headers
  -n,--no-verify              don't verify peer certificate when using HTTPS
  -v,--verbose                output verbose errors and action output

Subcommands:
  version                     Retrieve version information
  create                      Create various items, on and off the blockchain
  get                         Retrieve various items and information from the blockchain
  set                         Set or update blockchain state
  transfer                    Transfer tokens from account to account
  net                         Interact with local p2p network connections
  wallet                      Interact with local wallet
  sign                        Sign a transaction
  push                        Push arbitrary transactions to the blockchain
  multisig                    Multisig contract commands

```
To get help with any particular subcommand, run it with no arguments as well:
```
$ ./ncc create
Create various items, on and off the blockchain
Usage: ./ncc create SUBCOMMAND

Subcommands:
  key                         Create a new keypair and print the public and private keys
  account                     Create a new account on the blockchain (assumes system contract does not restrict RAM usage)

$ ./ncc create account
Create a new account on the blockchain (assumes system contract does not restrict RAM usage)
Usage: ./ncc create account [OPTIONS] creator name OwnerKey ActiveKey

Positionals:
  creator TEXT                The name of the account creating the new account
  name TEXT                   The name of the new account
  OwnerKey TEXT               The owner public key for the new account
  ActiveKey TEXT              The active public key for the new account

Options:
  -x,--expiration             set the time in seconds before a transaction expires, defaults to 30s
  -f,--force-unique           force the transaction to be unique. this will consume extra bandwidth and remove any protections against accidently issuing the same transaction multiple times
  -s,--skip-sign              Specify if unlocked wallet keys should be used to sign transaction
  -d,--dont-broadcast         don't broadcast transaction to the network (just print to stdout)
  -p,--permission TEXT ...    An account and permission level to authorize, as in 'account@permission' (defaults to 'creator@active')
```
*/

#include <pwd.h>
#include <string>
#include <vector>
#include <regex>
#include <iostream>

#define CLI11_HAS_FILESYSTEM 0
#include "CLI11.hpp"
#include "config.hpp"

#pragma push_macro("N")
#undef N

#include <boost/asio.hpp>
#include <boost/format.hpp>
#include <boost/dll/runtime_symbol_info.hpp>
#include <boost/filesystem.hpp>
#include <boost/process.hpp>
#include <boost/process/spawn.hpp>
#include <boost/range/algorithm/find_if.hpp>
#include <boost/range/algorithm/sort.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <boost/algorithm/string/classification.hpp>

using namespace std;
using namespace boost::filesystem;
using namespace nchain::client::config;
// namespace bip = boost::interprocess;
namespace bfs = boost::filesystem;

//copy pasta from keosd's main.cpp
bfs::path determine_home_directory()
{
   bfs::path home;
   struct passwd* pwd = getpwuid(getuid());
   if(pwd) {
      home = pwd->pw_dir;
   }
   else {
      home = getenv("HOME");
   }
   if(home.empty())
      home = "./";
   return home;
}

string url = "http://127.0.0.1:8888/";
string default_wallet_url = "unix://" + (determine_home_directory() / "nc-wallet" / (string(node_executable_name) + ".sock")).string();
string wallet_url; //to be set to default_wallet_url in main
bool no_verify = false;
vector<string> headers;

int main(int argc, char** argv) {
    CLI::App app{"Command Line Interface to nchain Client"};
    app.require_subcommand();

    std::string filename = "default";
    app.add_option("-f,--file", filename, "A help string");

    CLI11_PARSE(app, argc, argv);
    return 0;
}