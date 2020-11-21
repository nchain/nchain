// #include <eosio/chain/types.hpp>

#include <eosio/net_plugin/net_plugin.hpp>
#include <eosio/net_plugin/protocol.hpp>
// #include <eosio/chain/controller.hpp>
#include <eosio/chain/exceptions.hpp>
#include <eosio/chain/block.hpp>
#include <eosio/chain/plugin_interface.hpp>
#include <eosio/chain/thread_utils.hpp>
#include <eosio/producer_plugin/producer_plugin.hpp>
#include <eosio/chain/contract_types.hpp>
#include <eosio/chain/generated_transaction_object.hpp>
#include "connection.hpp"
#include "net_plugin_impl.hpp"

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
   static appbase::abstract_plugin& _net_plugin = app().register_plugin<net_plugin>();
   net_plugin_impl *my_impl;

   static const fc::string logger_name("net_plugin_impl");
   fc::logger logger = {};
   std::string peer_log_format = "";

   template<typename Function>
   void for_each_connection( Function f ) {
      std::shared_lock<std::shared_mutex> g( my_impl->connections_mtx );
      for( auto& c : my_impl->connections ) {
         if( !f( c ) ) return;
      }
   }

   template<typename Function>
   void for_each_block_connection( Function f ) {
      std::shared_lock<std::shared_mutex> g( my_impl->connections_mtx );
      for( auto& c : my_impl->connections ) {
         if( c->is_transactions_only_connection() ) continue;
         if( !f( c ) ) return;
      }
   }

   //-----------------------------------------------------------

    sync_manager::sync_manager( uint32_t req_span )
      :sync_known_lib_num( 0 )
      ,sync_last_requested_num( 0 )
      ,sync_next_expected_num( 1 )
      ,sync_req_span( req_span )
      ,sync_source()
      ,sync_state(in_sync)
   {
   }

   constexpr auto sync_manager::stage_str(stages s) {
    switch (s) {
    case in_sync : return "in sync";
    case lib_catchup: return "lib catchup";
    case head_catchup : return "head catchup";
    default : return "unkown";
    }
  }

   void sync_manager::set_state(stages newstate) {
      if( sync_state == newstate ) {
         return;
      }
      fc_ilog( logger, "old state ${os} becoming ${ns}", ("os", stage_str( sync_state ))( "ns", stage_str( newstate ) ) );
      sync_state = newstate;
   }

   void sync_manager::sync_reset_lib_num(const connection_ptr& c) {
      std::unique_lock<std::mutex> g( sync_mtx );
      if( sync_state == in_sync ) {
         sync_source.reset();
      }
      if( !c ) return;
      if( c->current() ) {
         std::lock_guard<std::mutex> g_conn( c->conn_mtx );
         if( c->last_handshake_recv.last_irreversible_block_num > sync_known_lib_num ) {
            sync_known_lib_num = c->last_handshake_recv.last_irreversible_block_num;
         }
      } else if( c == sync_source ) {
         sync_last_requested_num = 0;
         request_next_chunk( std::move(g) );
      }
   }

   // call with g_sync locked
   void sync_manager::request_next_chunk( std::unique_lock<std::mutex> g_sync, const connection_ptr& conn ) {
      uint32_t fork_head_block_num = 0;
      uint32_t lib_block_num = 0;
      std::tie( lib_block_num, std::ignore, fork_head_block_num,
                std::ignore, std::ignore, std::ignore ) = my_impl->get_chain_info();

      fc_dlog( logger, "sync_last_requested_num: ${r}, sync_next_expected_num: ${e}, sync_known_lib_num: ${k}, sync_req_span: ${s}",
               ("r", sync_last_requested_num)("e", sync_next_expected_num)("k", sync_known_lib_num)("s", sync_req_span) );

      if( fork_head_block_num < sync_last_requested_num && sync_source && sync_source->current() ) {
         fc_ilog( logger, "ignoring request, head is ${h} last req = ${r} source is ${p}",
                  ("h", fork_head_block_num)( "r", sync_last_requested_num )( "p", sync_source->peer_name() ) );
         return;
      }

      /* ----------
       * next chunk provider selection criteria
       * a provider is supplied and able to be used, use it.
       * otherwise select the next available from the list, round-robin style.
       */

      if (conn && conn->current() ) {
         sync_source = conn;
      } else {
         std::shared_lock<std::shared_mutex> g( my_impl->connections_mtx );
         if( my_impl->connections.size() == 0 ) {
            sync_source.reset();
         } else if( my_impl->connections.size() == 1 ) {
            if (!sync_source) {
               sync_source = *my_impl->connections.begin();
            }
         } else {
            // init to a linear array search
            auto cptr = my_impl->connections.begin();
            auto cend = my_impl->connections.end();
            // do we remember the previous source?
            if (sync_source) {
               //try to find it in the list
               cptr = my_impl->connections.find( sync_source );
               cend = cptr;
               if( cptr == my_impl->connections.end() ) {
                  //not there - must have been closed! cend is now connections.end, so just flatten the ring.
                  sync_source.reset();
                  cptr = my_impl->connections.begin();
               } else {
                  //was found - advance the start to the next. cend is the old source.
                  if( ++cptr == my_impl->connections.end() && cend != my_impl->connections.end() ) {
                     cptr = my_impl->connections.begin();
                  }
               }
            }

            //scan the list of peers looking for another able to provide sync blocks.
            if( cptr != my_impl->connections.end() ) {
               auto cstart_it = cptr;
               do {
                  //select the first one which is current and break out.
                  if( !(*cptr)->is_transactions_only_connection() && (*cptr)->current() ) {
                     sync_source = *cptr;
                     break;
                  }
                  if( ++cptr == my_impl->connections.end() )
                     cptr = my_impl->connections.begin();
               } while( cptr != cstart_it );
            }
            // no need to check the result, either source advanced or the whole list was checked and the old source is reused.
         }
      }

      // verify there is an available source
      if( !sync_source || !sync_source->current() || sync_source->is_transactions_only_connection() ) {
         fc_elog( logger, "Unable to continue syncing at this time");
         sync_known_lib_num = lib_block_num;
         sync_last_requested_num = 0;
         set_state( in_sync ); // probably not, but we can't do anything else
         return;
      }

      bool request_sent = false;
      if( sync_last_requested_num != sync_known_lib_num ) {
         uint32_t start = sync_next_expected_num;
         uint32_t end = start + sync_req_span - 1;
         if( end > sync_known_lib_num )
            end = sync_known_lib_num;
         if( end > 0 && end >= start ) {
            sync_last_requested_num = end;
            connection_ptr c = sync_source;
            g_sync.unlock();
            request_sent = true;
            c->strand->post( [c, start, end]() {
               fc_ilog( logger, "requesting range ${s} to ${e}, from ${n}", ("n", c->peer_name())( "s", start )( "e", end ) );
               c->request_sync_blocks( start, end );
            } );
         }
      }
      if( !request_sent ) {
         connection_ptr c = sync_source;
         g_sync.unlock();
         c->send_handshake();
      }
   }

   // static, thread safe
   void sync_manager::send_handshakes() {
      for_each_connection( []( auto& ci ) {
         if( ci->current() ) {
            ci->send_handshake();
         }
         return true;
      } );
   }

   bool sync_manager::is_sync_required( uint32_t fork_head_block_num ) {
      fc_dlog( logger, "last req = ${req}, last recv = ${recv} known = ${known} our head = ${head}",
               ("req", sync_last_requested_num)( "recv", sync_next_expected_num )( "known", sync_known_lib_num )
               ("head", fork_head_block_num ) );

      return( sync_last_requested_num < sync_known_lib_num ||
              fork_head_block_num < sync_last_requested_num );
   }

   void sync_manager::start_sync(const connection_ptr& c, uint32_t target) {
      std::unique_lock<std::mutex> g_sync( sync_mtx );
      if( target > sync_known_lib_num) {
         sync_known_lib_num = target;
      }

      uint32_t lib_num = 0;
      uint32_t fork_head_block_num = 0;
      std::tie( lib_num, std::ignore, fork_head_block_num,
                std::ignore, std::ignore, std::ignore ) = my_impl->get_chain_info();

      if( !is_sync_required( fork_head_block_num ) || target <= lib_num ) {
         fc_dlog( logger, "We are already caught up, my irr = ${b}, head = ${h}, target = ${t}",
                  ("b", lib_num)( "h", fork_head_block_num )( "t", target ) );
         return;
      }

      if( sync_state == in_sync ) {
         set_state( lib_catchup );
      }
      sync_next_expected_num = std::max( lib_num + 1, sync_next_expected_num );

      fc_ilog( logger, "Catching up with chain, our last req is ${cc}, theirs is ${t} peer ${p}",
               ("cc", sync_last_requested_num)( "t", target )( "p", c->peer_name() ) );

      request_next_chunk( std::move( g_sync ), c );
   }

   // called from connection strand
   void sync_manager::sync_reassign_fetch(const connection_ptr& c, go_away_reason reason) {
      std::unique_lock<std::mutex> g( sync_mtx );
      fc_ilog( logger, "reassign_fetch, our last req is ${cc}, next expected is ${ne} peer ${p}",
               ("cc", sync_last_requested_num)( "ne", sync_next_expected_num )( "p", c->peer_name() ) );

      if( c == sync_source ) {
         c->cancel_sync(reason);
         sync_last_requested_num = 0;
         request_next_chunk( std::move(g) );
      }
   }

   void sync_manager::recv_handshake( const connection_ptr& c, const handshake_message& msg ) {

      if( c->is_transactions_only_connection() ) return;

      uint32_t lib_num = 0;
      uint32_t peer_lib = msg.last_irreversible_block_num;
      uint32_t head = 0;
      block_id_type head_id;
      std::tie( lib_num, std::ignore, head,
                std::ignore, std::ignore, head_id ) = my_impl->get_chain_info();

      sync_reset_lib_num(c);

      //--------------------------------
      // sync need checks; (lib == last irreversible block)
      //
      // 0. my head block id == peer head id means we are all caught up block wise
      // 1. my head block num < peer lib - start sync locally
      // 2. my lib > peer head num - send an last_irr_catch_up notice if not the first generation
      //
      // 3  my head block num < peer head block num - update sync state and send a catchup request
      // 4  my head block num >= peer block num send a notice catchup if this is not the first generation
      //    4.1 if peer appears to be on a different fork ( our_id_for( msg.head_num ) != msg.head_id )
      //        then request peer's blocks
      //
      //-----------------------------

      if (head_id == msg.head_id) {
         fc_ilog( logger, "handshake from ${ep}, lib ${lib}, head ${head}, head id ${id}.. sync 0",
                  ("ep", c->peer_name())("lib", msg.last_irreversible_block_num)("head", msg.head_num)
                  ("id", msg.head_id.str().substr(8,16)) );
         c->syncing = false;
         notice_message note;
         note.known_blocks.mode = none;
         note.known_trx.mode = catch_up;
         note.known_trx.pending = 0;
         c->enqueue( note );
         return;
      }
      if (head < peer_lib) {
         fc_ilog( logger, "handshake from ${ep}, lib ${lib}, head ${head}, head id ${id}.. sync 1",
                  ("ep", c->peer_name())("lib", msg.last_irreversible_block_num)("head", msg.head_num)
                  ("id", msg.head_id.str().substr(8,16)) );
         c->syncing = false;
         // wait for receipt of a notice message before initiating sync
         if (c->protocol_version < proto_explicit_sync) {
            start_sync( c, peer_lib );
         }
         return;
      }
      if (lib_num > msg.head_num ) {
         fc_ilog( logger, "handshake from ${ep}, lib ${lib}, head ${head}, head id ${id}.. sync 2",
                  ("ep", c->peer_name())("lib", msg.last_irreversible_block_num)("head", msg.head_num)
                  ("id", msg.head_id.str().substr(8,16)) );
         if (msg.generation > 1 || c->protocol_version > proto_base) {
            notice_message note;
            note.known_trx.pending = lib_num;
            note.known_trx.mode = last_irr_catch_up;
            note.known_blocks.mode = last_irr_catch_up;
            note.known_blocks.pending = head;
            c->enqueue( note );
         }
         c->syncing = true;
         return;
      }

      if (head < msg.head_num ) {
         fc_ilog( logger, "handshake from ${ep}, lib ${lib}, head ${head}, head id ${id}.. sync 3",
                  ("ep", c->peer_name())("lib", msg.last_irreversible_block_num)("head", msg.head_num)
                  ("id", msg.head_id.str().substr(8,16)) );
         c->syncing = false;
         verify_catchup(c, msg.head_num, msg.head_id);
         return;
      } else {
         fc_ilog( logger, "handshake from ${ep}, lib ${lib}, head ${head}, head id ${id}.. sync 4",
                  ("ep", c->peer_name())("lib", msg.last_irreversible_block_num)("head", msg.head_num)
                  ("id", msg.head_id.str().substr(8,16)) );
         if (msg.generation > 1 ||  c->protocol_version > proto_base) {
            notice_message note;
            note.known_trx.mode = none;
            note.known_blocks.mode = catch_up;
            note.known_blocks.pending = head;
            note.known_blocks.ids.push_back(head_id);
            c->enqueue( note );
         }
         c->syncing = false;

         app().post( priority::medium, [chain_plug = my_impl->chain_plug, c,
                                        msg_head_num = msg.head_num, msg_head_id = msg.head_id]() {
            bool on_fork = true;
            try {
               controller& cc = chain_plug->chain();
               on_fork = cc.get_block_id_for_num( msg_head_num ) != msg_head_id;
            } catch( ... ) {}
            if( on_fork ) {
               c->strand->post( [c]() {
                  request_message req;
                  req.req_blocks.mode = catch_up;
                  req.req_trx.mode = none;
                  c->enqueue( req );
               } );
            }
         } );
         return;
      }
   }

   bool sync_manager::verify_catchup(const connection_ptr& c, uint32_t num, const block_id_type& id) {
      request_message req;
      req.req_blocks.mode = catch_up;
      for_each_block_connection( [num, &id, &req]( const auto& cc ) {
         std::lock_guard<std::mutex> g_conn( cc->conn_mtx );
         if( cc->fork_head_num > num || cc->fork_head == id ) {
            req.req_blocks.mode = none;
            return false;
         }
         return true;
      } );
      if( req.req_blocks.mode == catch_up ) {
         {
            std::lock_guard<std::mutex> g( sync_mtx );
            fc_ilog( logger, "catch_up while in ${s}, fork head num = ${fhn} "
                             "target LIB = ${lib} next_expected = ${ne}, id ${id}..., peer ${p}",
                     ("s", stage_str( sync_state ))("fhn", num)("lib", sync_known_lib_num)
                     ("ne", sync_next_expected_num)("id", id.str().substr( 8, 16 ))("p", c->peer_name()) );
         }
         uint32_t lib;
         block_id_type head_id;
         std::tie( lib, std::ignore, std::ignore,
                   std::ignore, std::ignore, head_id ) = my_impl->get_chain_info();
         if( sync_state == lib_catchup || num < lib )
            return false;
         set_state( head_catchup );
         {
            std::lock_guard<std::mutex> g_conn( c->conn_mtx );
            c->fork_head = id;
            c->fork_head_num = num;
         }

         req.req_blocks.ids.emplace_back( head_id );
      } else {
         fc_ilog( logger, "none notice while in ${s}, fork head num = ${fhn}, id ${id}..., peer ${p}",
                  ("s", stage_str( sync_state ))("fhn", num)
                  ("id", id.str().substr(8,16))("p", c->peer_name()) );
         std::lock_guard<std::mutex> g_conn( c->conn_mtx );
         c->fork_head = block_id_type();
         c->fork_head_num = 0;
      }
      req.req_trx.mode = none;
      c->enqueue( req );
      return true;
   }

   void sync_manager::sync_recv_notice( const connection_ptr& c, const notice_message& msg) {
      fc_dlog( logger, "sync_manager got ${m} block notice", ("m", modes_str( msg.known_blocks.mode )) );
      EOS_ASSERT( msg.known_blocks.mode == catch_up || msg.known_blocks.mode == last_irr_catch_up, plugin_exception,
                  "sync_recv_notice only called on catch_up" );
      if (msg.known_blocks.mode == catch_up) {
         if (msg.known_blocks.ids.size() == 0) {
            fc_elog( logger,"got a catch up with ids size = 0" );
         } else {
            const block_id_type& id = msg.known_blocks.ids.back();
            fc_ilog( logger, "notice_message, pending ${p}, blk_num ${n}, id ${id}...",
                     ("p", msg.known_blocks.pending)("n", block_header::num_from_id(id))("id",id.str().substr(8,16)) );
            if( !my_impl->dispatcher->have_block( id ) ) {
               verify_catchup( c, msg.known_blocks.pending, id );
            } else {
               // we already have the block, so update peer with our view of the world
               c->send_handshake();
            }
         }
      } else if (msg.known_blocks.mode == last_irr_catch_up) {
         {
            std::lock_guard<std::mutex> g_conn( c->conn_mtx );
            c->last_handshake_recv.last_irreversible_block_num = msg.known_trx.pending;
         }
         sync_reset_lib_num(c);
         start_sync(c, msg.known_trx.pending);
      }
   }

   // called from connection strand
   void sync_manager::rejected_block( const connection_ptr& c, uint32_t blk_num ) {
      std::unique_lock<std::mutex> g( sync_mtx );
      if( ++c->consecutive_rejected_blocks > def_max_consecutive_rejected_blocks ) {
         fc_wlog( logger, "block ${bn} not accepted from ${p}, closing connection", ("bn", blk_num)("p", c->peer_name()) );
         sync_last_requested_num = 0;
         sync_source.reset();
         g.unlock();
         c->close();
      } else {
         c->send_handshake( true );
      }
   }

   // called from connection strand
   void sync_manager::sync_update_expected( const connection_ptr& c, const block_id_type& blk_id, uint32_t blk_num, bool blk_applied ) {
      std::unique_lock<std::mutex> g_sync( sync_mtx );
      if( blk_num <= sync_last_requested_num ) {
         fc_dlog( logger, "sync_last_requested_num: ${r}, sync_next_expected_num: ${e}, sync_known_lib_num: ${k}, sync_req_span: ${s}",
                  ("r", sync_last_requested_num)("e", sync_next_expected_num)("k", sync_known_lib_num)("s", sync_req_span) );
         if (blk_num != sync_next_expected_num && !blk_applied) {
            auto sync_next_expected = sync_next_expected_num;
            g_sync.unlock();
            fc_dlog( logger, "expected block ${ne} but got ${bn}, from connection: ${p}",
                     ("ne", sync_next_expected)( "bn", blk_num )( "p", c->peer_name() ) );
            return;
         }
         sync_next_expected_num = blk_num + 1;
      }
   }

   // called from connection strand
   void sync_manager::sync_recv_block(const connection_ptr& c, const block_id_type& blk_id, uint32_t blk_num, bool blk_applied) {
      fc_dlog( logger, "got block ${bn} from ${p}", ("bn", blk_num)( "p", c->peer_name() ) );
      if( app().is_quiting() ) {
         c->close( false, true );
         return;
      }
      c->consecutive_rejected_blocks = 0;
      sync_update_expected( c, blk_id, blk_num, blk_applied );
      std::unique_lock<std::mutex> g_sync( sync_mtx );
      stages state = sync_state;
      fc_dlog( logger, "state ${s}", ("s", stage_str( state )) );
      if( state == head_catchup ) {
         fc_dlog( logger, "sync_manager in head_catchup state" );
         sync_source.reset();
         g_sync.unlock();

         block_id_type null_id;
         bool set_state_to_head_catchup = false;
         for_each_block_connection( [&null_id, blk_num, &blk_id, &c, &set_state_to_head_catchup]( const auto& cp ) {
            std::unique_lock<std::mutex> g_cp_conn( cp->conn_mtx );
            uint32_t fork_head_num = cp->fork_head_num;
            block_id_type fork_head_id = cp->fork_head;
            g_cp_conn.unlock();
            if( fork_head_id == null_id ) {
               // continue
            } else if( fork_head_num < blk_num || fork_head_id == blk_id ) {
               std::lock_guard<std::mutex> g_conn( c->conn_mtx );
               c->fork_head = null_id;
               c->fork_head_num = 0;
            } else {
               set_state_to_head_catchup = true;
            }
            return true;
         } );

         if( set_state_to_head_catchup ) {
            set_state( head_catchup );
         } else {
            set_state( in_sync );
            send_handshakes();
         }
      } else if( state == lib_catchup ) {
         if( blk_num == sync_known_lib_num ) {
            fc_dlog( logger, "All caught up with last known last irreversible block resending handshake" );
            set_state( in_sync );
            g_sync.unlock();
            send_handshakes();
         } else if( blk_num == sync_last_requested_num ) {
            request_next_chunk( std::move( g_sync) );
         } else {
            g_sync.unlock();
            fc_dlog( logger, "calling sync_wait on connection ${p}", ("p", c->peer_name()) );
            c->sync_wait();
         }
      }
   }

   //------------------------------------------------------------------------

   // thread safe
   bool dispatch_manager::add_peer_block( const block_id_type& blkid, uint32_t connection_id) {
      std::lock_guard<std::mutex> g( blk_state_mtx );
      auto bptr = blk_state.get<by_id>().find( std::make_tuple( connection_id, std::ref( blkid )));
      bool added = (bptr == blk_state.end());
      if( added ) {
         blk_state.insert( {blkid, block_header::num_from_id( blkid ), connection_id, true} );
      } else if( !bptr->have_block ) {
         blk_state.modify( bptr, []( auto& pb ) {
            pb.have_block = true;
         });
      }
      return added;
   }

   bool dispatch_manager::peer_has_block( const block_id_type& blkid, uint32_t connection_id ) const {
      std::lock_guard<std::mutex> g(blk_state_mtx);
      const auto blk_itr = blk_state.get<by_id>().find( std::make_tuple( connection_id, std::ref( blkid )));
      return blk_itr != blk_state.end();
   }

   bool dispatch_manager::have_block( const block_id_type& blkid ) const {
      std::lock_guard<std::mutex> g(blk_state_mtx);
      // by_block_id sorts have_block by greater so have_block == true will be the first one found
      const auto& index = blk_state.get<by_block_id>();
      auto blk_itr = index.find( blkid );
      if( blk_itr != index.end() ) {
         return blk_itr->have_block;
      }
      return false;
   }

   bool dispatch_manager::add_peer_txn( const node_transaction_state& nts ) {
      std::lock_guard<std::mutex> g( local_txns_mtx );
      auto tptr = local_txns.get<by_id>().find( std::make_tuple( std::ref( nts.id ), nts.connection_id ) );
      bool added = (tptr == local_txns.end());
      if( added ) {
         local_txns.insert( nts );
      }
      return added;
   }

   // thread safe
   void dispatch_manager::update_txns_block_num( const signed_block_ptr& sb ) {
      update_block_num ubn( sb->block_num() );
      std::lock_guard<std::mutex> g( local_txns_mtx );
      for( const auto& recpt : sb->transactions ) {
         const transaction_id_type& id = (recpt.trx.which() == 0) ? recpt.trx.get<transaction_id_type>()
                                                                  : recpt.trx.get<packed_transaction>().id();
         auto range = local_txns.get<by_id>().equal_range( id );
         for( auto itr = range.first; itr != range.second; ++itr ) {
            local_txns.modify( itr, ubn );
         }
      }
   }

   // thread safe
   void dispatch_manager::update_txns_block_num( const transaction_id_type& id, uint32_t blk_num ) {
      update_block_num ubn( blk_num );
      std::lock_guard<std::mutex> g( local_txns_mtx );
      auto range = local_txns.get<by_id>().equal_range( id );
      for( auto itr = range.first; itr != range.second; ++itr ) {
         local_txns.modify( itr, ubn );
      }
   }

   bool dispatch_manager::peer_has_txn( const transaction_id_type& tid, uint32_t connection_id ) const {
      std::lock_guard<std::mutex> g( local_txns_mtx );
      const auto tptr = local_txns.get<by_id>().find( std::make_tuple( std::ref( tid ), connection_id ) );
      return tptr != local_txns.end();
   }

   bool dispatch_manager::have_txn( const transaction_id_type& tid ) const {
      std::lock_guard<std::mutex> g( local_txns_mtx );
      const auto tptr = local_txns.get<by_id>().find( tid );
      return tptr != local_txns.end();
   }

   void dispatch_manager::expire_txns( uint32_t lib_num ) {
      size_t start_size = 0, end_size = 0;

      std::unique_lock<std::mutex> g( local_txns_mtx );
      start_size = local_txns.size();
      auto& old = local_txns.get<by_expiry>();
      auto ex_lo = old.lower_bound( fc::time_point_sec( 0 ) );
      auto ex_up = old.upper_bound( time_point::now() );
      old.erase( ex_lo, ex_up );
      g.unlock(); // allow other threads opportunity to use local_txns

      g.lock();
      auto& stale = local_txns.get<by_block_num>();
      stale.erase( stale.lower_bound( 1 ), stale.upper_bound( lib_num ) );
      end_size = local_txns.size();
      g.unlock();

      fc_dlog( logger, "expire_local_txns size ${s} removed ${r}", ("s", start_size)( "r", start_size - end_size ) );
   }

   void dispatch_manager::expire_blocks( uint32_t lib_num ) {
      std::lock_guard<std::mutex> g(blk_state_mtx);
      auto& stale_blk = blk_state.get<by_block_num>();
      stale_blk.erase( stale_blk.lower_bound(1), stale_blk.upper_bound(lib_num) );
   }

   // thread safe
   void dispatch_manager::bcast_block(const signed_block_ptr& b, const block_id_type& id) {
      fc_dlog( logger, "bcast block ${b}", ("b", b->block_num()) );

      if( my_impl->sync_master->syncing_with_peer() ) return;

      bool have_connection = false;
      for_each_block_connection( [&have_connection]( auto& cp ) {
         peer_dlog( cp, "connected ${s}, connecting ${c}, syncing ${ss}",
                    ("s", cp->connected())("c", cp->connecting.load())("ss", cp->syncing.load()) );

         if( !cp->current() ) {
            return true;
         }
         have_connection = true;
         return false;
      } );

      if( !have_connection ) return;
      std::shared_ptr<std::vector<char>> send_buffer = create_send_buffer( b );

      for_each_block_connection( [this, &id, bnum = b->block_num(), &send_buffer]( auto& cp ) {
         if( !cp->current() ) {
            return true;
         }
         cp->strand->post( [this, cp, id, bnum, send_buffer]() {
            std::unique_lock<std::mutex> g_conn( cp->conn_mtx );
            bool has_block = cp->last_handshake_recv.last_irreversible_block_num >= bnum;
            g_conn.unlock();
            if( !has_block ) {
               if( !add_peer_block( id, cp->connection_id ) ) {
                  fc_dlog( logger, "not bcast block ${b} to ${p}", ("b", bnum)("p", cp->peer_name()) );
                  return;
               }
               fc_dlog( logger, "bcast block ${b} to ${p}", ("b", bnum)("p", cp->peer_name()) );
               cp->enqueue_buffer( send_buffer, no_reason );
            }
         });
         return true;
      } );
   }

   // called from connection strand
   void dispatch_manager::recv_block(const connection_ptr& c, const block_id_type& id, uint32_t bnum) {
      std::unique_lock<std::mutex> g( c->conn_mtx );
      if (c &&
          c->last_req &&
          c->last_req->req_blocks.mode != none &&
          !c->last_req->req_blocks.ids.empty() &&
          c->last_req->req_blocks.ids.back() == id) {
         fc_dlog( logger, "reseting last_req" );
         c->last_req.reset();
      }
      g.unlock();

      fc_dlog(logger, "canceling wait on ${p}", ("p",c->peer_name()));
      c->cancel_wait();
   }

   void dispatch_manager::rejected_block(const block_id_type& id) {
      fc_dlog( logger, "rejected block ${id}", ("id", id) );
   }

   void dispatch_manager::bcast_transaction(const packed_transaction& trx) {
      const auto& id = trx.id();
      time_point_sec trx_expiration = trx.expiration();
      node_transaction_state nts = {id, trx_expiration, 0, 0};

      std::shared_ptr<std::vector<char>> send_buffer;
      for_each_connection( [this, &trx, &nts, &send_buffer]( auto& cp ) {
         if( cp->is_blocks_only_connection() || !cp->current() ) {
            return true;
         }
         nts.connection_id = cp->connection_id;
         if( !add_peer_txn(nts) ) {
            return true;
         }
         if( !send_buffer ) {
            send_buffer = create_send_buffer( trx );
         }

         cp->strand->post( [cp, send_buffer]() {
            fc_dlog( logger, "sending trx to ${n}", ("n", cp->peer_name()) );
            cp->enqueue_buffer( send_buffer, no_reason );
         } );
         return true;
      } );
   }

   void dispatch_manager::rejected_transaction(const packed_transaction_ptr& trx, uint32_t head_blk_num) {
      fc_dlog( logger, "not sending rejected transaction ${tid}", ("tid", trx->id()) );
      // keep rejected transaction around for awhile so we don't broadcast it
      // update its block number so it will be purged when current block number is lib
      if( trx->expiration() > fc::time_point::now() ) { // no need to update blk_num if already expired
         update_txns_block_num( trx->id(), head_blk_num );
      }
   }

   // called from connection strand
   void dispatch_manager::recv_notice(const connection_ptr& c, const notice_message& msg, bool generated) {
      if (msg.known_trx.mode == normal) {
      } else if (msg.known_trx.mode != none) {
         fc_elog( logger, "passed a notice_message with something other than a normal on none known_trx" );
         return;
      }
      if (msg.known_blocks.mode == normal) {
         // known_blocks.ids is never > 1
         if( !msg.known_blocks.ids.empty() ) {
            if( msg.known_blocks.pending == 1 ) { // block id notify of 2.0.0, ignore
               return;
            }
         }
      } else if (msg.known_blocks.mode != none) {
         fc_elog( logger, "passed a notice_message with something other than a normal on none known_blocks" );
         return;
      }
   }

   void dispatch_manager::retry_fetch(const connection_ptr& c) {
      fc_dlog( logger, "retry fetch" );
      request_message last_req;
      block_id_type bid;
      {
         std::lock_guard<std::mutex> g_c_conn( c->conn_mtx );
         if( !c->last_req ) {
            return;
         }
         fc_wlog( logger, "failed to fetch from ${p}", ("p", c->peer_address()) );
         if( c->last_req->req_blocks.mode == normal && !c->last_req->req_blocks.ids.empty() ) {
            bid = c->last_req->req_blocks.ids.back();
         } else {
            fc_wlog( logger, "no retry, block mpde = ${b} trx mode = ${t}",
                     ("b", modes_str( c->last_req->req_blocks.mode ))( "t", modes_str( c->last_req->req_trx.mode ) ) );
            return;
         }
         last_req = *c->last_req;
      }
      for_each_block_connection( [this, &c, &last_req, &bid]( auto& conn ) {
         if( conn == c )
            return true;
         {
            std::lock_guard<std::mutex> guard( conn->conn_mtx );
            if( conn->last_req ) {
               return true;
            }
         }

         bool sendit = peer_has_block( bid, conn->connection_id );
         if( sendit ) {
            conn->strand->post( [conn, last_req{std::move(last_req)}]() {
               conn->enqueue( last_req );
               conn->fetch_wait();
               std::lock_guard<std::mutex> g_conn_conn( conn->conn_mtx );
               conn->last_req = last_req;
            } );
            return false;
         }
         return true;
      } );

      // at this point no other peer has it, re-request or do nothing?
      fc_wlog( logger, "no peer has last_req" );
      if( c->connected() ) {
         c->enqueue( last_req );
         c->fetch_wait();
      }
   }

   void net_plugin_impl::start_listen_loop() {
       strand->post([this]() {
           listener->accept(boost::asio::bind_executor(
               *strand, [this](boost::system::error_code ec, std::shared_ptr<net_stream> stream,
                               const std::string &remote_addr) {
                   if (!ec) {
                       if (!stream) {
                           return; // ignore error happen
                       }

                       uint32_t visitors  = 0;
                       uint32_t from_addr = 0;
                       if (!remote_addr.empty()) {
                           for_each_connection([&visitors, &from_addr, &remote_addr](auto &conn) {
                               if (conn->connected()) {
                                   if (conn->peer_address().empty()) {
                                       ++visitors;
                                       std::lock_guard<std::mutex> g_conn(conn->conn_mtx);
                                       if (remote_addr == conn->get_remote_endpoint_ip()) {
                                           ++from_addr;
                                       }
                                   }
                               }
                               return true;
                           });
                           if (from_addr < max_nodes_per_host &&
                               (max_client_count == 0 || visitors < max_client_count)) {
                               connection_ptr new_connection = std::make_shared<connection>(stream);

                               // new_connection->connecting = true;
                               fc_ilog(logger, "Accepted new connection: " + remote_addr);
                               if (new_connection->start_session()) {
                                   std::lock_guard<std::shared_mutex> g_unique(connections_mtx);
                                   connections.insert(new_connection);
                               }

                           } else {
                               if (from_addr >= max_nodes_per_host) {
                                   fc_dlog(
                                       logger,
                                       "Number of connections (${n}) from ${ra} exceeds limit ${l}",
                                       ("n", from_addr + 1)("ra", remote_addr)("l",
                                                                               max_nodes_per_host));
                               } else {
                                   fc_dlog(logger, "max_client_count ${m} exceeded",
                                           ("m", max_client_count));
                               }
                               // stream never added to connections and start_session not called,
                               // lifetime will end
                               stream->close();
                           }
                       }
                   } else {
                       fc_elog(logger, "Error accepting connection: ${m}", ("m", ec.message()));
                       // For the listed error codes below, recall start_listen_loop()
                       switch (ec.value()) {
                       case ECONNABORTED:
                       case EMFILE:
                       case ENFILE:
                       case ENOBUFS:
                       case ENOMEM:
                       case EPROTO:
                           break;
                       default:
                           return;
                       }
                   }
                   start_listen_loop();
               }));
       });
   }

   // call only from main application thread
   void net_plugin_impl::update_chain_info() {

      controller& cc = chain_plug->chain();
      std::lock_guard<std::mutex> g( chain_info_mtx );
      chain_lib_num = cc.last_irreversible_block_num();
      chain_lib_id = cc.last_irreversible_block_id();
      chain_head_blk_num = cc.head_block_num();
      chain_head_blk_id = cc.head_block_id();
      chain_fork_head_blk_num = cc.fork_db_pending_head_block_num();
      chain_fork_head_blk_id = cc.fork_db_pending_head_block_id();
      fc_dlog( logger, "updating chain info lib ${lib}, head ${head}, fork ${fork}",
               ("lib", chain_lib_num)("head", chain_head_blk_num)("fork", chain_fork_head_blk_num) );
   }

   //         lib_num, head_blk_num, fork_head_blk_num, lib_id, head_blk_id, fork_head_blk_id
   std::tuple<uint32_t, uint32_t, uint32_t, block_id_type, block_id_type, block_id_type>
   net_plugin_impl::get_chain_info() const {
      std::lock_guard<std::mutex> g( chain_info_mtx );
      return std::make_tuple(
            chain_lib_num, chain_head_blk_num, chain_fork_head_blk_num,
            chain_lib_id, chain_head_blk_id, chain_fork_head_blk_id );
   }

   // called from any thread
   void net_plugin_impl::start_conn_timer(boost::asio::steady_timer::duration du, std::weak_ptr<connection> from_connection) {
      if( in_shutdown ) return;
      std::lock_guard<std::mutex> g( connector_check_timer_mtx );
      ++connector_checks_in_flight;
      connector_check_timer->expires_from_now( du );
      connector_check_timer->async_wait( [my = shared_from_this(), from_connection](boost::system::error_code ec) {
            std::unique_lock<std::mutex> g( my->connector_check_timer_mtx );
            int num_in_flight = --my->connector_checks_in_flight;
            g.unlock();
            if( !ec ) {
               my->connection_monitor(from_connection, num_in_flight == 0 );
            } else {
               if( num_in_flight == 0 ) {
                  if( my->in_shutdown ) return;
                  fc_elog( logger, "Error from connection check monitor: ${m}", ("m", ec.message()));
                  my->start_conn_timer( my->connector_period, std::weak_ptr<connection>() );
               }
            }
      });
   }

   // thread safe
   void net_plugin_impl::start_expire_timer() {
      if( in_shutdown ) return;
      std::lock_guard<std::mutex> g( expire_timer_mtx );
      expire_timer->expires_from_now( txn_exp_period);
      expire_timer->async_wait( [my = shared_from_this()]( boost::system::error_code ec ) {
         if( !ec ) {
            my->expire();
         } else {
            if( my->in_shutdown ) return;
            fc_elog( logger, "Error from transaction check monitor: ${m}", ("m", ec.message()) );
            my->start_expire_timer();
         }
      } );
   }

   // thread safe
   void net_plugin_impl::ticker() {
      if( in_shutdown ) return;
      std::lock_guard<std::mutex> g( keepalive_timer_mtx );
      keepalive_timer->expires_from_now(keepalive_interval);
      keepalive_timer->async_wait( [my = shared_from_this()]( boost::system::error_code ec ) {
            my->ticker();
            if( ec ) {
               if( my->in_shutdown ) return;
               fc_wlog( logger, "Peer keepalive ticked sooner than expected: ${m}", ("m", ec.message()) );
            }
            for_each_connection( []( auto& c ) {
               if( c->connected() ) {
                  c->strand->post( [c]() {
                     c->send_time();
                  } );
               }
               return true;
            } );
         } );
   }

   void net_plugin_impl::start_monitors() {
      {
         std::lock_guard<std::mutex> g( connector_check_timer_mtx );
         connector_check_timer.reset(new boost::asio::steady_timer( my_impl->thread_pool->get_executor() ));
      }
      {
         std::lock_guard<std::mutex> g( expire_timer_mtx );
         expire_timer.reset( new boost::asio::steady_timer( my_impl->thread_pool->get_executor() ) );
      }
      start_conn_timer(connector_period, std::weak_ptr<connection>());
      start_expire_timer();
   }

   void net_plugin_impl::expire() {
      auto now = time_point::now();
      uint32_t lib = 0;
      std::tie( lib, std::ignore, std::ignore, std::ignore, std::ignore, std::ignore ) = get_chain_info();
      dispatcher->expire_blocks( lib );
      dispatcher->expire_txns( lib );
      fc_dlog( logger, "expire_txns ${n}us", ("n", time_point::now() - now) );

      start_expire_timer();
   }

   // called from any thread
   void net_plugin_impl::connection_monitor(std::weak_ptr<connection> from_connection, bool reschedule ) {
      auto max_time = fc::time_point::now();
      max_time += fc::milliseconds(max_cleanup_time_ms);
      auto from = from_connection.lock();
      std::unique_lock<std::shared_mutex> g( connections_mtx );
      auto it = (from ? connections.find(from) : connections.begin());
      if (it == connections.end()) it = connections.begin();
      size_t num_rm = 0, num_clients = 0, num_peers = 0;
      while (it != connections.end()) {
         if (fc::time_point::now() >= max_time) {
            connection_wptr wit = *it;
            g.unlock();
            fc_dlog( logger, "Exiting connection monitor early, ran out of time: ${t}", ("t", max_time - fc::time_point::now()) );
            if( reschedule ) {
               start_conn_timer( std::chrono::milliseconds( 1 ), wit ); // avoid exhausting
            }
            return;
         }
         (*it)->peer_address().empty() ? ++num_clients : ++num_peers;
         if( !(*it)->connected() && !(*it)->connecting) {
            if( !(*it)->peer_address().empty() ) {
               if( !(*it)->resolve_and_connect() ) {
                  it = connections.erase(it);
                  --num_peers; ++num_rm;
                  continue;
               }
            } else {
               --num_clients; ++num_rm;
               it = connections.erase(it);
               continue;
            }
         }
         ++it;
      }
      g.unlock();
      if( num_clients > 0 || num_peers > 0 )
         fc_ilog( logger, "p2p client connections: ${num}/${max}, peer connections: ${pnum}/${pmax}",
                  ("num", num_clients)("max", max_client_count)("pnum", num_peers)("pmax", supplied_peers.size()) );
      fc_dlog( logger, "connection monitor, removed ${n} connections", ("n", num_rm) );
      if( reschedule ) {
         start_conn_timer( connector_period, std::weak_ptr<connection>());
      }
   }

   // called from application thread
   void net_plugin_impl::on_accepted_block(const block_state_ptr& bs) {
      update_chain_info();

      controller& cc = chain_plug->chain();
      dispatcher->strand.post( [this, bs]() {
         fc_dlog( logger, "signaled accepted_block, blk num = ${num}, id = ${id}", ("num", bs->block_num)("id", bs->id) );
         dispatcher->bcast_block( bs->block, bs->id );
      });
   }

   // called from application thread
   void net_plugin_impl::on_pre_accepted_block(const signed_block_ptr& block) {
      update_chain_info();

      controller& cc = chain_plug->chain();
      if( cc.is_trusted_producer(block->producer) ) {
         dispatcher->strand.post( [this, block]() {
            auto id = block->id();
            fc_dlog( logger, "signaled pre_accepted_block, blk num = ${num}, id = ${id}", ("num", block->block_num())("id", id) );
            dispatcher->bcast_block( block, id );
         });
      }
   }

   // called from application thread
   void net_plugin_impl::on_irreversible_block( const block_state_ptr& block) {
      fc_dlog( logger, "on_irreversible_block, blk num = ${num}, id = ${id}", ("num", block->block_num)("id", block->id) );
      update_chain_info();
   }

   // called from application thread
   void net_plugin_impl::transaction_ack(const std::pair<fc::exception_ptr, transaction_metadata_ptr>& results) {
      dispatcher->strand.post( [this, results]() {
         const auto& id = results.second->id();
         if (results.first) {
            fc_dlog( logger, "signaled NACK, trx-id = ${id} : ${why}", ("id", id)( "why", results.first->to_detail_string() ) );

            uint32_t head_blk_num = 0;
            std::tie( std::ignore, head_blk_num, std::ignore, std::ignore, std::ignore, std::ignore ) = get_chain_info();
            dispatcher->rejected_transaction(results.second->packed_trx(), head_blk_num);
         } else {
            fc_dlog( logger, "signaled ACK, trx-id = ${id}", ("id", id) );
            dispatcher->bcast_transaction(*results.second->packed_trx());
         }
      });
   }

   bool net_plugin_impl::authenticate_peer(const handshake_message& msg) const {
      if(allowed_connections == None)
         return false;

      if(allowed_connections == Any)
         return true;


      if(allowed_connections & (Producers | Specified)) {
         auto allowed_it = std::find(allowed_peers.begin(), allowed_peers.end(), msg.key);
         auto private_it = private_keys.find(msg.key);

         bool found_producer_key = false;
         if(producer_plug != nullptr)
            found_producer_key = producer_plug->is_producer_key(msg.key);
         if( allowed_it == allowed_peers.end() && private_it == private_keys.end() && !found_producer_key) {
            fc_elog( logger, "Peer ${peer} sent a handshake with an unauthorized key: ${key}.",
                     ("peer", msg.p2p_address)("key", msg.key) );
            return false;
         }
      }

      namespace sc = std::chrono;
      sc::system_clock::duration msg_time(msg.time);
      auto time = sc::system_clock::now().time_since_epoch();
      if(time - msg_time > peer_authentication_interval) {
         fc_elog( logger, "Peer ${peer} sent a handshake with a timestamp skewed by more than ${time}.",
                  ("peer", msg.p2p_address)("time", "1 second")); // TODO Add to_variant for std::chrono::system_clock::duration
         return false;
      }

      if(msg.sig != chain::signature_type() && msg.token != fc::sha256()) {
         fc::sha256 hash = fc::sha256::hash(msg.time);
         if(hash != msg.token) {
            fc_elog( logger, "Peer ${peer} sent a handshake with an invalid token.", ("peer", msg.p2p_address) );
            return false;
         }
         chain::public_key_type peer_key;
         try {
            peer_key = fc::crypto::public_key(msg.sig, msg.token, true);
         }
         catch (fc::exception& /*e*/) {
            fc_elog( logger, "Peer ${peer} sent a handshake with an unrecoverable key.", ("peer", msg.p2p_address) );
            return false;
         }
         if((allowed_connections & (Producers | Specified)) && peer_key != msg.key) {
            fc_elog( logger, "Peer ${peer} sent a handshake with an unauthenticated key.", ("peer", msg.p2p_address) );
            return false;
         }
      }
      else if(allowed_connections & (Producers | Specified)) {
         fc_dlog( logger, "Peer sent a handshake with blank signature and token, but this node accepts only authenticated connections." );
         return false;
      }
      return true;
   }

   chain::public_key_type net_plugin_impl::get_authentication_key() const {
      if(!private_keys.empty())
         return private_keys.begin()->first;
      /*producer_plugin* pp = app().find_plugin<producer_plugin>();
      if(pp != nullptr && pp->get_state() == abstract_plugin::started)
         return pp->first_producer_public_key();*/
      return chain::public_key_type();
   }

   chain::signature_type net_plugin_impl::sign_compact(const chain::public_key_type& signer, const fc::sha256& digest) const
   {
      auto private_key_itr = private_keys.find(signer);
      if(private_key_itr != private_keys.end())
         return private_key_itr->second.sign(digest);

      if(producer_plug != nullptr && producer_plug->get_state() == abstract_plugin::started)
         return producer_plug->sign_compact(signer, digest);
      return chain::signature_type();
   }

   net_plugin::net_plugin()
      :my( new net_plugin_impl ) {
      my_impl = my.get();
   }

   net_plugin::~net_plugin() {
   }

   void net_plugin::set_program_options( options_description& /*cli*/, options_description& cfg )
   {
      cfg.add_options()
         ( "p2p-listen-endpoint", bpo::value<string>()->default_value( "0.0.0.0:9876" ), "The actual host:port used to listen for incoming p2p connections.")
         ( "p2p-server-address", bpo::value<string>(), "An externally accessible host:port for identifying this node. Defaults to p2p-listen-endpoint.")
         ( "p2p-peer-address", bpo::value< vector<string> >()->composing(),
           "The public endpoint of a peer node to connect to. Use multiple p2p-peer-address options as needed to compose a network.\n"
           "  Syntax: host:port[:<trx>|<blk>]\n"
           "  The optional 'trx' and 'blk' indicates to node that only transactions 'trx' or blocks 'blk' should be sent."
           "  Examples:\n"
           "    p2p.eos.io:9876\n"
           "    p2p.trx.eos.io:9876:trx\n"
           "    p2p.blk.eos.io:9876:blk\n")
         ( "p2p-max-nodes-per-host", bpo::value<int>()->default_value(def_max_nodes_per_host), "Maximum number of client nodes from any single IP address")
         ( "p2p-accept-transactions", bpo::value<bool>()->default_value(true), "Allow transactions received over p2p network to be evaluated and relayed if valid.")
         ( "agent-name", bpo::value<string>()->default_value("\"EOS Test Agent\""), "The name supplied to identify this node amongst the peers.")
         ( "allowed-connection", bpo::value<vector<string>>()->multitoken()->default_value({"any"}, "any"), "Can be 'any' or 'producers' or 'specified' or 'none'. If 'specified', peer-key must be specified at least once. If only 'producers', peer-key is not required. 'producers' and 'specified' may be combined.")
         ( "peer-key", bpo::value<vector<string>>()->composing()->multitoken(), "Optional public key of peer allowed to connect.  May be used multiple times.")
         ( "peer-private-key", boost::program_options::value<vector<string>>()->composing()->multitoken(),
           "Tuple of [PublicKey, WIF private key] (may specify multiple times)")
         ( "max-clients", bpo::value<int>()->default_value(def_max_clients), "Maximum number of clients from which connections are accepted, use 0 for no limit")
         ( "connection-cleanup-period", bpo::value<int>()->default_value(def_conn_retry_wait), "number of seconds to wait before cleaning up dead connections")
         ( "max-cleanup-time-msec", bpo::value<int>()->default_value(10), "max connection cleanup time per cleanup call in millisec")
         ( "net-threads", bpo::value<uint16_t>()->default_value(my->thread_pool_size),
           "Number of worker threads in net_plugin thread pool" )
         ( "sync-fetch-span", bpo::value<uint32_t>()->default_value(def_sync_fetch_span), "number of blocks to retrieve in a chunk from any individual peer during synchronization")
         ( "use-socket-read-watermark", bpo::value<bool>()->default_value(false), "Enable experimental socket read watermark optimization")
         ( "peer-log-format", bpo::value<string>()->default_value( "[\"${_name}\" ${_ip}:${_port}]" ),
           "The string used to format peers when logging messages about them.  Variables are escaped with ${<variable name>}.\n"
           "Available Variables:\n"
           "   _name  \tself-reported name\n\n"
           "   _id    \tself-reported ID (64 hex characters)\n\n"
           "   _sid   \tfirst 8 characters of _peer.id\n\n"
           "   _ip    \tremote IP address of peer\n\n"
           "   _port  \tremote port number of peer\n\n"
           "   _lip   \tlocal IP address connected to peer\n\n"
           "   _lport \tlocal port number connected to peer\n\n")
        ;
   }

   template<typename T>
   T dejsonify(const string& s) {
      return fc::json::from_string(s).as<T>();
   }

   void net_plugin::plugin_initialize( const variables_map& options ) {
      fc_ilog( logger, "Initialize net plugin" );
      try {
         peer_log_format = options.at( "peer-log-format" ).as<string>();

         my->sync_master.reset( new sync_manager( options.at( "sync-fetch-span" ).as<uint32_t>()));

         my->connector_period = std::chrono::seconds( options.at( "connection-cleanup-period" ).as<int>());
         my->max_cleanup_time_ms = options.at("max-cleanup-time-msec").as<int>();
         my->txn_exp_period = def_txn_expire_wait;
         my->resp_expected_period = def_resp_expected_wait;
         my->max_client_count = options.at( "max-clients" ).as<int>();
         my->max_nodes_per_host = options.at( "p2p-max-nodes-per-host" ).as<int>();
         my->p2p_accept_transactions = options.at( "p2p-accept-transactions" ).as<bool>();

         my->use_socket_read_watermark = options.at( "use-socket-read-watermark" ).as<bool>();

         if( options.count( "p2p-listen-endpoint" ) && options.at("p2p-listen-endpoint").as<string>().length()) {
            my->p2p_address = options.at( "p2p-listen-endpoint" ).as<string>();
            EOS_ASSERT( my->p2p_address.length() <= max_p2p_address_length, chain::plugin_config_exception,
                        "p2p-listen-endpoint to long, must be less than ${m}", ("m", max_p2p_address_length) );
         }
         if( options.count( "p2p-server-address" ) ) {
            my->p2p_server_address = options.at( "p2p-server-address" ).as<string>();
            EOS_ASSERT( my->p2p_server_address.length() <= max_p2p_address_length, chain::plugin_config_exception,
                        "p2p_server_address to long, must be less than ${m}", ("m", max_p2p_address_length) );
         }

         my->thread_pool_size = options.at( "net-threads" ).as<uint16_t>();
         EOS_ASSERT( my->thread_pool_size > 0, chain::plugin_config_exception,
                     "net-threads ${num} must be greater than 0", ("num", my->thread_pool_size) );

         if( options.count( "p2p-peer-address" )) {
            my->supplied_peers = options.at( "p2p-peer-address" ).as<vector<string> >();
         }
         if( options.count( "agent-name" )) {
            my->user_agent_name = options.at( "agent-name" ).as<string>();
            EOS_ASSERT( my->user_agent_name.length() <= max_handshake_str_length, chain::plugin_config_exception,
                        "agent-name to long, must be less than ${m}", ("m", max_handshake_str_length) );
         }

         if( options.count( "allowed-connection" )) {
            const std::vector<std::string> allowed_remotes = options["allowed-connection"].as<std::vector<std::string>>();
            for( const std::string& allowed_remote : allowed_remotes ) {
               if( allowed_remote == "any" )
                  my->allowed_connections |= net_plugin_impl::Any;
               else if( allowed_remote == "producers" )
                  my->allowed_connections |= net_plugin_impl::Producers;
               else if( allowed_remote == "specified" )
                  my->allowed_connections |= net_plugin_impl::Specified;
               else if( allowed_remote == "none" )
                  my->allowed_connections = net_plugin_impl::None;
            }
         }

         if( my->allowed_connections & net_plugin_impl::Specified )
            EOS_ASSERT( options.count( "peer-key" ),
                        plugin_config_exception,
                       "At least one peer-key must accompany 'allowed-connection=specified'" );

         if( options.count( "peer-key" )) {
            const std::vector<std::string> key_strings = options["peer-key"].as<std::vector<std::string>>();
            for( const std::string& key_string : key_strings ) {
               my->allowed_peers.push_back( dejsonify<chain::public_key_type>( key_string ));
            }
         }

         if( options.count( "peer-private-key" )) {
            const std::vector<std::string> key_id_to_wif_pair_strings = options["peer-private-key"].as<std::vector<std::string>>();
            for( const std::string& key_id_to_wif_pair_string : key_id_to_wif_pair_strings ) {
               auto key_id_to_wif_pair = dejsonify<std::pair<chain::public_key_type, std::string>>(
                     key_id_to_wif_pair_string );
               my->private_keys[key_id_to_wif_pair.first] = fc::crypto::private_key( key_id_to_wif_pair.second );
            }
         }


         my->chain_plug = app().find_plugin<chain_plugin>();
         EOS_ASSERT( my->chain_plug, chain::missing_chain_plugin_exception, ""  );
         my->chain_id = my->chain_plug->get_chain_id();
         fc::rand_pseudo_bytes( my->node_id.data(), my->node_id.data_size());
         const controller& cc = my->chain_plug->chain();

         if( cc.get_read_mode() == db_read_mode::IRREVERSIBLE || cc.get_read_mode() == db_read_mode::READ_ONLY ) {
            if( my->p2p_accept_transactions ) {
               my->p2p_accept_transactions = false;
               string m = cc.get_read_mode() == db_read_mode::IRREVERSIBLE ? "irreversible" : "read-only";
               wlog( "p2p-accept-transactions set to false due to read-mode: ${m}", ("m", m) );
            }
         }
         if( my->p2p_accept_transactions ) {
            my->chain_plug->enable_accept_transactions();
         }

      } FC_LOG_AND_RETHROW()
   }

   void net_plugin::plugin_startup() {
      handle_sighup();
      try {

      fc_ilog( logger, "my node_id is ${id}", ("id", my->node_id ));

      my->producer_plug = app().find_plugin<producer_plugin>();

      my->thread_pool.emplace( "net", my->thread_pool_size );

      my->dispatcher.reset( new dispatch_manager( my_impl->thread_pool->get_executor() ) );

      if( !my->p2p_accept_transactions && my->p2p_address.size() ) {
         fc_ilog( logger, "\n"
               "***********************************\n"
               "* p2p-accept-transactions = false *\n"
               "*    Transactions not forwarded   *\n"
               "***********************************\n" );
      }
      my->strand = std::make_shared<strand_t>(my_impl->thread_pool->get_executor());
      my->listener = std::make_shared<tcp_listener>();
      if (my->listener->init(my->strand)) {
         my->start_listen_loop();
      }

      // tcp::endpoint listen_endpoint;
      // if( my->p2p_address.size() > 0 ) {
      //    auto host = my->p2p_address.substr( 0, my->p2p_address.find( ':' ));
      //    auto port = my->p2p_address.substr( host.size() + 1, my->p2p_address.size());
      //    tcp::resolver::query query( tcp::v4(), host.c_str(), port.c_str());
      //    // Note: need to add support for IPv6 too?

      //    tcp::resolver resolver( my->thread_pool->get_executor() );
      //    listen_endpoint = *resolver.resolve( query );

      //    my->acceptor.reset( new tcp::acceptor( my_impl->thread_pool->get_executor() ) );

      //    if( !my->p2p_server_address.empty() ) {
      //       my->p2p_address = my->p2p_server_address;
      //    } else {
      //       if( listen_endpoint.address().to_v4() == address_v4::any()) {
      //          boost::system::error_code ec;
      //          auto host = host_name( ec );
      //          if( ec.value() != boost::system::errc::success ) {

      //             FC_THROW_EXCEPTION( fc::invalid_arg_exception,
      //                                 "Unable to retrieve host_name. ${msg}", ("msg", ec.message()));

      //          }
      //          auto port = my->p2p_address.substr( my->p2p_address.find( ':' ), my->p2p_address.size());
      //          my->p2p_address = host + port;
      //       }
      //    }
      // }

      // if( my->acceptor ) {
      //    try {
      //      my->acceptor->open(listen_endpoint.protocol());
      //      my->acceptor->set_option(tcp::acceptor::reuse_address(true));
      //      my->acceptor->bind(listen_endpoint);
      //      my->acceptor->listen();
      //    } catch (const std::exception& e) {
      //      elog( "net_plugin::plugin_startup failed to bind to port ${port}", ("port", listen_endpoint.port()) );
      //      throw e;
      //    }
      //    fc_ilog( logger, "starting listener, max clients is ${mc}",("mc",my->max_client_count) );
      //    my->start_listen_loop();
      // }

      {

         chain::controller& cc = my->chain_plug->chain();
         cc.accepted_block.connect( [my = my]( const block_state_ptr& s ) {
            my->on_accepted_block( s );
         } );
         cc.pre_accepted_block.connect( [my = my]( const signed_block_ptr& s ) {
            my->on_pre_accepted_block( s );
         } );
         cc.irreversible_block.connect( [my = my]( const block_state_ptr& s ) {
            my->on_irreversible_block( s );
         } );
      }

      {
         std::lock_guard<std::mutex> g( my->keepalive_timer_mtx );
         my->keepalive_timer.reset( new boost::asio::steady_timer( my->thread_pool->get_executor() ) );
      }
      my->ticker();

      my->incoming_transaction_ack_subscription = app().get_channel<compat::channels::transaction_ack>().subscribe(
            std::bind(&net_plugin_impl::transaction_ack, my.get(), std::placeholders::_1));

      my->start_monitors();

      my->update_chain_info();

      for( const auto& seed_node : my->supplied_peers ) {
         connect( seed_node );
      }

      } catch( ... ) {
         // always want plugin_shutdown even on exception
         plugin_shutdown();
         throw;
      }
   }

   void net_plugin::handle_sighup() {
      fc::logger::update( logger_name, logger );
   }

   void net_plugin::plugin_shutdown() {
      try {
         fc_ilog( logger, "shutdown.." );
         my->in_shutdown = true;
         {
            std::lock_guard<std::mutex> g( my->connector_check_timer_mtx );
            if( my->connector_check_timer )
               my->connector_check_timer->cancel();
         }{
            std::lock_guard<std::mutex> g( my->expire_timer_mtx );
            if( my->expire_timer )
               my->expire_timer->cancel();
         }{
            std::lock_guard<std::mutex> g( my->keepalive_timer_mtx );
            if( my->keepalive_timer )
               my->keepalive_timer->cancel();
         }

         {
            fc_ilog( logger, "close ${s} connections", ("s", my->connections.size()) );
            std::lock_guard<std::shared_mutex> g( my->connections_mtx );
            for( auto& con : my->connections ) {
               fc_dlog( logger, "close: ${p}", ("p", con->peer_name()) );
               con->close( false, true );
            }
            my->connections.clear();
         }

         if( my->thread_pool ) {
            my->thread_pool->stop();
         }

         if (my->listener) {
            my->listener->close();
            my->listener = nullptr;
         }

         app().post( 0, [me = my](){} ); // keep my pointer alive until queue is drained
         fc_ilog( logger, "exit shutdown" );
      }
      FC_CAPTURE_AND_RETHROW()
   }

   /**
    *  Used to trigger a new connection from RPC API
    */
   string net_plugin::connect( const string& host ) {
      std::lock_guard<std::shared_mutex> g( my->connections_mtx );
      if( my->find_connection( host ) )
         return "already connected";

      connection_ptr c = std::make_shared<connection>( host );
      fc_dlog( logger, "calling active connector: ${h}", ("h", host) );
      if( c->resolve_and_connect() ) {
         fc_dlog( logger, "adding new connection to the list: ${c}", ("c", c->peer_name()) );
         my->connections.insert( c );
      }
      return "added connection";
   }

   string net_plugin::disconnect( const string& host ) {
      std::lock_guard<std::shared_mutex> g( my->connections_mtx );
      for( auto itr = my->connections.begin(); itr != my->connections.end(); ++itr ) {
         if( (*itr)->peer_address() == host ) {
            fc_ilog( logger, "disconnecting: ${p}", ("p", (*itr)->peer_name()) );
            (*itr)->close();
            my->connections.erase(itr);
            return "connection removed";
         }
      }
      return "no known connection for host";
   }

   optional<connection_status> net_plugin::status( const string& host )const {
      std::shared_lock<std::shared_mutex> g( my->connections_mtx );
      auto con = my->find_connection( host );
      if( con )
         return con->get_status();
      return optional<connection_status>();
   }

   vector<connection_status> net_plugin::connections()const {
      vector<connection_status> result;
      std::shared_lock<std::shared_mutex> g( my->connections_mtx );
      result.reserve( my->connections.size() );
      for( const auto& c : my->connections ) {
         result.push_back( c->get_status() );
      }
      return result;
   }

   // call with connections_mtx
   connection_ptr net_plugin_impl::find_connection( const string& host )const {
      for( const auto& c : connections )
         if( c->peer_address() == host ) return c;
      return connection_ptr();
   }

}
