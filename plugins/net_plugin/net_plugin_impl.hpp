// Copyright (c) 2009-2010 Satoshi Nakamoto
// Copyright (c) 2020-2021 The nchain Developers
// Distributed under the MIT/X11 software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#pragma once

#include <eosio/net_plugin/net_plugin.hpp>
#include <eosio/net_plugin/protocol.hpp>
// #include <eosio/chain/controller.hpp>
#include <eosio/chain/exceptions.hpp>
#include <eosio/chain/block.hpp>
#include <eosio/chain/plugin_interface.hpp>
#include <eosio/chain/thread_utils.hpp>
// #include <eosio/producer_plugin/producer_plugin.hpp>
#include <eosio/chain/contract_types.hpp>
#include <eosio/chain/generated_transaction_object.hpp>
#include "connection.hpp"

#include <fc/network/message_buffer.hpp>
#include <fc/network/ip.hpp>
#include <fc/io/json.hpp>
#include <fc/io/raw.hpp>
#include <fc/log/appender.hpp>
#include <fc/log/logger_config.hpp>
#include <fc/reflect/variant.hpp>
#include <fc/crypto/rand.hpp>
#include <fc/exception/exception.hpp>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ip/host_name.hpp>
#include <boost/asio/steady_timer.hpp>

#include <atomic>
#include <shared_mutex>

using namespace eosio::chain::plugin_interface;

namespace eosio {
   using std::vector;

   using boost::asio::ip::tcp;
   using boost::asio::ip::address_v4;
   using boost::asio::ip::host_name;
   using boost::multi_index_container;

   using fc::time_point;
   using fc::time_point_sec;
   using eosio::chain::transaction_id_type;
   using eosio::chain::sha256_less;

   class connection;

   using connection_ptr = std::shared_ptr<connection>;
   using connection_wptr = std::weak_ptr<connection>;

   using io_work_t = boost::asio::executor_work_guard<boost::asio::io_context::executor_type>;

   template <typename Strand>
   void verify_strand_in_this_thread(const Strand& strand, const char* func, int line) {
      if( !strand.running_in_this_thread() ) {
         elog( "wrong strand: ${f} : line ${n}, exiting", ("f", func)("n", line) );
         app().quit();
      }
   }

   struct node_transaction_state {
      transaction_id_type id;
      time_point_sec  expires;        /// time after which this may be purged.
      uint32_t        block_num = 0;  /// block transaction was included in
      uint32_t        connection_id = 0;
   };

   struct by_expiry;
   struct by_block_num;

   typedef multi_index_container<
      node_transaction_state,
      indexed_by<
         ordered_unique<
            tag<by_id>,
            composite_key< node_transaction_state,
               member<node_transaction_state, transaction_id_type, &node_transaction_state::id>,
               member<node_transaction_state, uint32_t, &node_transaction_state::connection_id>
            >,
            composite_key_compare< sha256_less, std::less<uint32_t> >
         >,
         ordered_non_unique<
            tag< by_expiry >,
            member< node_transaction_state, fc::time_point_sec, &node_transaction_state::expires > >,
         ordered_non_unique<
            tag<by_block_num>,
            member< node_transaction_state, uint32_t, &node_transaction_state::block_num > >
         >
      >
   node_transaction_index;

   struct peer_block_state {
      block_id_type id;
      uint32_t      block_num = 0;
      uint32_t      connection_id = 0;
      bool          have_block = false; // true if we have received the block, false if only received id notification
   };

   struct by_block_id;

   typedef multi_index_container<
      eosio::peer_block_state,
      indexed_by<
         ordered_unique< tag<by_id>,
               composite_key< peer_block_state,
                     member<peer_block_state, uint32_t, &eosio::peer_block_state::connection_id>,
                     member<peer_block_state, block_id_type, &eosio::peer_block_state::id>
               >,
               composite_key_compare< std::less<uint32_t>, sha256_less >
         >,
         ordered_non_unique< tag<by_block_id>,
               composite_key< peer_block_state,
                     member<peer_block_state, block_id_type, &eosio::peer_block_state::id>,
                     member<peer_block_state, bool, &eosio::peer_block_state::have_block>
               >,
               composite_key_compare< sha256_less, std::greater<bool> >
         >,
         ordered_non_unique< tag<by_block_num>, member<eosio::peer_block_state, uint32_t, &eosio::peer_block_state::block_num > >
      >
      > peer_block_state_index;


   struct update_block_num {
      uint32_t new_bnum;
      update_block_num(uint32_t bnum) : new_bnum(bnum) {}
      void operator() (node_transaction_state& nts) {
         nts.block_num = new_bnum;
      }
   };

   class sync_manager {
   private:
      enum stages {
         lib_catchup,
         head_catchup,
         in_sync
      };

      mutable std::mutex sync_mtx;
      uint32_t       sync_known_lib_num{0};
      uint32_t       sync_last_requested_num{0};
      uint32_t       sync_next_expected_num{0};
      uint32_t       sync_req_span{0};
      connection_ptr sync_source;
      std::atomic<stages> sync_state{in_sync};

   private:
      constexpr static auto stage_str( stages s );
      void set_state( stages s );
      bool is_sync_required( uint32_t fork_head_block_num );
      void request_next_chunk( std::unique_lock<std::mutex> g_sync, const connection_ptr& conn = connection_ptr() );
      void start_sync( const connection_ptr& c, uint32_t target );
      bool verify_catchup( const connection_ptr& c, uint32_t num, const block_id_type& id );

   public:
      explicit sync_manager( uint32_t span );
      static void send_handshakes();
      bool syncing_with_peer() const { return sync_state == lib_catchup; }
      void sync_reset_lib_num( const connection_ptr& conn );
      void sync_reassign_fetch( const connection_ptr& c, go_away_reason reason );
      void rejected_block( const connection_ptr& c, uint32_t blk_num );
      void sync_recv_block( const connection_ptr& c, const block_id_type& blk_id, uint32_t blk_num, bool blk_applied );
      void sync_update_expected( const connection_ptr& c, const block_id_type& blk_id, uint32_t blk_num, bool blk_applied );
      void recv_handshake( const connection_ptr& c, const handshake_message& msg );
      void sync_recv_notice( const connection_ptr& c, const notice_message& msg );
   };

   class dispatch_manager {
      mutable std::mutex      blk_state_mtx;
      peer_block_state_index  blk_state;
      mutable std::mutex      local_txns_mtx;
      node_transaction_index  local_txns;

   public:
      boost::asio::io_context::strand  strand;

      explicit dispatch_manager(boost::asio::io_context& io_context)
      : strand( io_context ) {}

      void bcast_transaction(const packed_transaction& trx);
      void rejected_transaction(const packed_transaction_ptr& trx, uint32_t head_blk_num);
      void bcast_block( const signed_block_ptr& b, const block_id_type& id );
      void bcast_notice( const block_id_type& id );
      void rejected_block(const block_id_type& id);

      void recv_block(const connection_ptr& conn, const block_id_type& msg, uint32_t bnum);
      void expire_blocks( uint32_t bnum );
      void recv_notice(const connection_ptr& conn, const notice_message& msg, bool generated);

      void retry_fetch(const connection_ptr& conn);

      bool add_peer_block( const block_id_type& blkid, uint32_t connection_id );
      bool peer_has_block(const block_id_type& blkid, uint32_t connection_id) const;
      bool have_block(const block_id_type& blkid) const;

      bool add_peer_txn( const node_transaction_state& nts );
      void update_txns_block_num( const signed_block_ptr& sb );
      void update_txns_block_num( const transaction_id_type& id, uint32_t blk_num );
      bool peer_has_txn( const transaction_id_type& tid, uint32_t connection_id ) const;
      bool have_txn( const transaction_id_type& tid ) const;
      void expire_txns( uint32_t lib_num );
   };

   class net_plugin_impl : public std::enable_shared_from_this<net_plugin_impl> {
   public:
      unique_ptr<tcp::acceptor>        acceptor;
      std::atomic<uint32_t>            current_connection_id{0};

      unique_ptr< sync_manager >       sync_master;
      unique_ptr< dispatch_manager >   dispatcher;

      /**
       * Thread safe, only updated in plugin initialize
       *  @{
       */
      string                                p2p_address;
      string                                p2p_server_address;

      vector<string>                        supplied_peers;
      vector<chain::public_key_type>        allowed_peers; ///< peer keys allowed to connect
      std::map<chain::public_key_type,
               chain::private_key_type>     private_keys; ///< overlapping with producer keys, also authenticating non-producing nodes
      enum possible_connections : char {
         None = 0,
            Producers = 1 << 0,
            Specified = 1 << 1,
            Any = 1 << 2
            };
      possible_connections                  allowed_connections{None};

      boost::asio::steady_timer::duration   connector_period{0};
      boost::asio::steady_timer::duration   txn_exp_period{0};
      boost::asio::steady_timer::duration   resp_expected_period{0};
      boost::asio::steady_timer::duration   keepalive_interval{std::chrono::seconds{32}};

      int                                   max_cleanup_time_ms = 0;
      uint32_t                              max_client_count = 0;
      uint32_t                              max_nodes_per_host = 1;
      bool                                  p2p_accept_transactions = true;

      /// Peer clock may be no more than 1 second skewed from our clock, including network latency.
      const std::chrono::system_clock::duration peer_authentication_interval{std::chrono::seconds{1}};

      chain_id_type                         chain_id;
      fc::sha256                            node_id;
      string                                user_agent_name;

      // chain_plugin*                         chain_plug = nullptr;
      // producer_plugin*                      producer_plug = nullptr;
      bool                                  use_socket_read_watermark = false;
      /** @} */

      mutable std::shared_mutex             connections_mtx;
      std::set< connection_ptr >            connections;     // todo: switch to a thread safe container to avoid big mutex over complete collection

      std::mutex                            connector_check_timer_mtx;
      unique_ptr<boost::asio::steady_timer> connector_check_timer;
      int                                   connector_checks_in_flight{0};

      std::mutex                            expire_timer_mtx;
      unique_ptr<boost::asio::steady_timer> expire_timer;

      std::mutex                            keepalive_timer_mtx;
      unique_ptr<boost::asio::steady_timer> keepalive_timer;

      std::atomic<bool>                     in_shutdown{false};

      compat::channels::transaction_ack::channel_type::handle  incoming_transaction_ack_subscription;

      uint16_t                                  thread_pool_size = 2;
      optional<eosio::chain::named_thread_pool> thread_pool;

   private:
      mutable std::mutex            chain_info_mtx; // protects chain_*
      uint32_t                      chain_lib_num{0};
      uint32_t                      chain_head_blk_num{0};
      uint32_t                      chain_fork_head_blk_num{0};
      block_id_type                 chain_lib_id;
      block_id_type                 chain_head_blk_id;
      block_id_type                 chain_fork_head_blk_id;

   public:
      void update_chain_info();
      //         lib_num, head_block_num, fork_head_blk_num, lib_id, head_blk_id, fork_head_blk_id
      std::tuple<uint32_t, uint32_t, uint32_t, block_id_type, block_id_type, block_id_type> get_chain_info() const;

      void start_listen_loop();

      void on_accepted_block( const block_state_ptr& bs );
      void on_pre_accepted_block( const signed_block_ptr& bs );
      void transaction_ack(const std::pair<fc::exception_ptr, transaction_metadata_ptr>&);
      void on_irreversible_block( const block_state_ptr& blk );

      void start_conn_timer(boost::asio::steady_timer::duration du, std::weak_ptr<connection> from_connection);
      void start_expire_timer();
      void start_monitors();

      void expire();
      void connection_monitor(std::weak_ptr<connection> from_connection, bool reschedule);
      /** \name Peer Timestamps
       *  Time message handling
       *  @{
       */
      /** \brief Peer heartbeat ticker.
       */
      void ticker();
      /** @} */
      /** \brief Determine if a peer is allowed to connect.
       *
       * Checks current connection mode and key authentication.
       *
       * \return False if the peer should not connect, true otherwise.
       */
      bool authenticate_peer(const handshake_message& msg) const;
      /** \brief Retrieve public key used to authenticate with peers.
       *
       * Finds a key to use for authentication.  If this node is a producer, use
       * the front of the producer key map.  If the node is not a producer but has
       * a configured private key, use it.  If the node is neither a producer nor has
       * a private key, returns an empty key.
       *
       * \note On a node with multiple private keys configured, the key with the first
       *       numerically smaller byte will always be used.
       */
      chain::public_key_type get_authentication_key() const;
      /** \brief Returns a signature of the digest using the corresponding private key of the signer.
       *
       * If there are no configured private keys, returns an empty signature.
       */
      chain::signature_type sign_compact(const chain::public_key_type& signer, const fc::sha256& digest) const;

      inline constexpr uint16_t to_protocol_version(uint16_t v) {
         if (v >= net_version_base) {
            v -= net_version_base;
            return (v > net_version_range) ? 0 : v;
         }
         return 0;
      }

      connection_ptr find_connection(const string& host)const; // must call with held mutex
   };

   const fc::string logger_name("net_plugin_impl");
   fc::logger logger;
   std::string peer_log_format;

#define peer_dlog( PEER, FORMAT, ... ) \
  FC_MULTILINE_MACRO_BEGIN \
   if( logger.is_enabled( fc::log_level::debug ) ) \
      logger.log( FC_LOG_MESSAGE( debug, peer_log_format + FORMAT, __VA_ARGS__ (PEER->get_logger_variant()) ) ); \
  FC_MULTILINE_MACRO_END

#define peer_ilog( PEER, FORMAT, ... ) \
  FC_MULTILINE_MACRO_BEGIN \
   if( logger.is_enabled( fc::log_level::info ) ) \
      logger.log( FC_LOG_MESSAGE( info, peer_log_format + FORMAT, __VA_ARGS__ (PEER->get_logger_variant()) ) ); \
  FC_MULTILINE_MACRO_END

#define peer_wlog( PEER, FORMAT, ... ) \
  FC_MULTILINE_MACRO_BEGIN \
   if( logger.is_enabled( fc::log_level::warn ) ) \
      logger.log( FC_LOG_MESSAGE( warn, peer_log_format + FORMAT, __VA_ARGS__ (PEER->get_logger_variant()) ) ); \
  FC_MULTILINE_MACRO_END

#define peer_elog( PEER, FORMAT, ... ) \
  FC_MULTILINE_MACRO_BEGIN \
   if( logger.is_enabled( fc::log_level::error ) ) \
      logger.log( FC_LOG_MESSAGE( error, peer_log_format + FORMAT, __VA_ARGS__ (PEER->get_logger_variant())) ); \
  FC_MULTILINE_MACRO_END


   template<class enum_type, class=typename std::enable_if<std::is_enum<enum_type>::value>::type>
   inline enum_type& operator|=(enum_type& lhs, const enum_type& rhs)
   {
      using T = std::underlying_type_t <enum_type>;
      return lhs = static_cast<enum_type>(static_cast<T>(lhs) | static_cast<T>(rhs));
   }

} // namespace eosio
