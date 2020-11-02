#pragma once
#include <appbase/application.hpp>
// #include <eosio/chain_plugin/chain_plugin.hpp>
#include <eosio/chain/name.hpp>
#include <eosio/chain/exceptions.hpp>
#include <eosio/net_plugin/protocol.hpp>
#include <eosio/net_plugin/connection_base.hpp>

namespace eosio {
   using namespace appbase;

   class net_plugin : public appbase::plugin<net_plugin>
   {
      public:
        net_plugin();
        virtual ~net_plugin();

        // APPBASE_PLUGIN_REQUIRES((chain_plugin))
        APPBASE_PLUGIN_REQUIRES()
        virtual void set_program_options(options_description& cli, options_description& cfg) override;
        void handle_sighup() override;

        void plugin_initialize(const variables_map& options);
        void plugin_startup();
        void plugin_shutdown();

        string                       connect( const string& endpoint );
        string                       disconnect( const string& endpoint );
        optional<connection_status>  status( const string& endpoint )const;
        vector<connection_status>    connections()const;

      private:
        std::shared_ptr<class net_plugin_impl> my;
   };

}

FC_REFLECT( eosio::connection_status, (peer)(connecting)(syncing)(last_handshake) )
