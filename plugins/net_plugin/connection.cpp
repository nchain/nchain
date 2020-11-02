// Copyright (c) 2009-2010 Satoshi Nakamoto
// Copyright (c) 2020-2021 The nchain Developers
// Distributed under the MIT/X11 software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.
// #include <eosio/chain/types.hpp>

#include "connection.hpp"
#include "net_plugin_impl.hpp"

namespace eosio {

const string connection::unknown = "<unknown>";

extern net_plugin_impl *my_impl;

std::shared_ptr<std::vector<char>> create_send_buffer( const signed_block_ptr& sb ) {
    // this implementation is to avoid copy of signed_block to net_message
    // matches which of net_message for signed_block
    fc_dlog( logger, "sending block ${bn}", ("bn", sb->block_num()) );
    return create_send_buffer( signed_block_which, *sb );
}

std::shared_ptr<std::vector<char>> create_send_buffer( const packed_transaction& trx ) {
    // this implementation is to avoid copy of packed_transaction to net_message
    // matches which of net_message for packed_transaction
    return create_send_buffer( packed_transaction_which, trx );
}

// called from connection strand
struct msg_handler : public fc::visitor<void> {
    connection_ptr c;
    explicit msg_handler( const connection_ptr& conn) : c(conn) {}

    template<typename T>
    void operator()( const T& ) const {
        EOS_ASSERT( false, plugin_config_exception, "Not implemented, call handle_message directly instead" );
    }

    void operator()( const handshake_message& msg ) const {
        // continue call to handle_message on connection strand
        fc_dlog( logger, "handle handshake_message" );
        c->handle_message( msg );
    }

    void operator()( const chain_size_message& msg ) const {
        // continue call to handle_message on connection strand
        fc_dlog( logger, "handle chain_size_message" );
        c->handle_message( msg );
    }

    void operator()( const go_away_message& msg ) const {
        // continue call to handle_message on connection strand
        fc_dlog( logger, "handle go_away_message" );
        c->handle_message( msg );
    }

    void operator()( const time_message& msg ) const {
        // continue call to handle_message on connection strand
        fc_dlog( logger, "handle time_message" );
        c->handle_message( msg );
    }

    void operator()( const notice_message& msg ) const {
        // continue call to handle_message on connection strand
        fc_dlog( logger, "handle notice_message" );
        c->handle_message( msg );
    }

    void operator()( const request_message& msg ) const {
        // continue call to handle_message on connection strand
        fc_dlog( logger, "handle request_message" );
        c->handle_message( msg );
    }

    void operator()( const sync_request_message& msg ) const {
        // continue call to handle_message on connection strand
        fc_dlog( logger, "handle sync_request_message" );
        c->handle_message( msg );
    }
};

//---------------------------------------------------------------------------

connection::connection( string endpoint )
    : peer_addr( endpoint ),
    strand( my_impl->thread_pool->get_executor() ),
    socket( new tcp::socket( my_impl->thread_pool->get_executor() ) ),
    connection_id( ++my_impl->current_connection_id ),
    response_expected_timer( my_impl->thread_pool->get_executor() ),
    last_handshake_recv(),
    last_handshake_sent()
{
    fc_ilog( logger, "creating connection to ${n}", ("n", endpoint) );
}

connection::connection()
    : peer_addr(),
    strand( my_impl->thread_pool->get_executor() ),
    socket( new tcp::socket( my_impl->thread_pool->get_executor() ) ),
    connection_id( ++my_impl->current_connection_id ),
    response_expected_timer( my_impl->thread_pool->get_executor() ),
    last_handshake_recv(),
    last_handshake_sent()
{
    fc_dlog( logger, "new connection object created" );
}

void connection::update_endpoints() {
    boost::system::error_code ec;
    boost::system::error_code ec2;
    auto rep = socket->remote_endpoint(ec);
    auto lep = socket->local_endpoint(ec2);
    std::lock_guard<std::mutex> g_conn( conn_mtx );
    remote_endpoint_ip = ec ? unknown : rep.address().to_string();
    remote_endpoint_port = ec ? unknown : std::to_string(rep.port());
    local_endpoint_ip = ec2 ? unknown : lep.address().to_string();
    local_endpoint_port = ec2 ? unknown : std::to_string(lep.port());
}

void connection::set_connection_type( const string& peer_add ) {
    // host:port:[<trx>|<blk>]
    string::size_type colon = peer_add.find(':');
    string::size_type colon2 = peer_add.find(':', colon + 1);
    string::size_type end = colon2 == string::npos
        ? string::npos : peer_add.find_first_of( " :+=.,<>!$%^&(*)|-#@\t", colon2 + 1 ); // future proof by including most symbols without using regex
    string host = peer_add.substr( 0, colon );
    string port = peer_add.substr( colon + 1, colon2 == string::npos ? string::npos : colon2 - (colon + 1));
    string type = colon2 == string::npos ? "" : end == string::npos ?
        peer_add.substr( colon2 + 1 ) : peer_add.substr( colon2 + 1, end - (colon2 + 1) );

    if( type.empty() ) {
        fc_dlog( logger, "Setting connection type for: ${peer} to both transactions and blocks", ("peer", peer_add) );
        connection_type = both;
    } else if( type == "trx" ) {
        fc_dlog( logger, "Setting connection type for: ${peer} to transactions only", ("peer", peer_add) );
        connection_type = transactions_only;
    } else if( type == "blk" ) {
        fc_dlog( logger, "Setting connection type for: ${peer} to blocks only", ("peer", peer_add) );
        connection_type = blocks_only;
    } else {
        fc_wlog( logger, "Unknown connection type: ${t}", ("t", type) );
    }
}

connection_status connection::get_status()const {
    connection_status stat;
    stat.peer = peer_addr;
    stat.connecting = connecting;
    stat.syncing = syncing;
    std::lock_guard<std::mutex> g( conn_mtx );
    stat.last_handshake = last_handshake_recv;
    return stat;
}

bool connection::start_session() {
    verify_strand_in_this_thread( strand, __func__, __LINE__ );

    update_endpoints();
    boost::asio::ip::tcp::no_delay nodelay( true );
    boost::system::error_code ec;
    socket->set_option( nodelay, ec );
    if( ec ) {
        fc_elog( logger, "connection failed (set_option) ${peer}: ${e1}", ("peer", peer_name())( "e1", ec.message() ) );
        close();
        return false;
    } else {
        fc_dlog( logger, "connected to ${peer}", ("peer", peer_name()) );
        socket_open = true;
        start_read_message();
        return true;
    }
}

bool connection::connected() {
    return socket_is_open() && !connecting;
}

bool connection::current() {
    return (connected() && !syncing);
}

void connection::flush_queues() {
    buffer_queue.clear_write_queue();
}

void connection::close( bool reconnect, bool shutdown ) {
    strand.post( [self = shared_from_this(), reconnect, shutdown]() {
        connection::_close( self.get(), reconnect, shutdown );
    });
}

void connection::_close( connection* self, bool reconnect, bool shutdown ) {
    self->socket_open = false;
    boost::system::error_code ec;
    if( self->socket->is_open() ) {
        self->socket->shutdown( tcp::socket::shutdown_both, ec );
        self->socket->close( ec );
    }
    self->socket.reset( new tcp::socket( my_impl->thread_pool->get_executor() ) );
    self->flush_queues();
    self->connecting = false;
    self->syncing = false;
    self->consecutive_rejected_blocks = 0;
    ++self->consecutive_immediate_connection_close;
    bool has_last_req = false;
    {
        std::lock_guard<std::mutex> g_conn( self->conn_mtx );
        has_last_req = !!self->last_req;
        self->last_handshake_recv = handshake_message();
        self->last_handshake_sent = handshake_message();
        self->last_close = fc::time_point::now();
        self->conn_node_id = fc::sha256();
    }
    if( has_last_req && !shutdown ) {
        my_impl->dispatcher->retry_fetch( self->shared_from_this() );
    }
    self->peer_requested.reset();
    self->sent_handshake_count = 0;
    if( !shutdown) my_impl->sync_master->sync_reset_lib_num( self->shared_from_this() );
    fc_ilog( logger, "closing '${a}', ${p}", ("a", self->peer_address())("p", self->peer_name()) );
    fc_dlog( logger, "canceling wait on ${p}", ("p", self->peer_name()) ); // peer_name(), do not hold conn_mtx
    self->cancel_wait();

    if( reconnect && !shutdown ) {
        my_impl->start_conn_timer( std::chrono::milliseconds( 100 ), connection_wptr() );
    }
}

void connection::blk_send_branch( const block_id_type& msg_head_id ) {
    uint32_t head_num = 0;
    std::tie( std::ignore, std::ignore, head_num,
            std::ignore, std::ignore, std::ignore ) = my_impl->get_chain_info();

    fc_dlog(logger, "head_num = ${h}",("h",head_num));
    if(head_num == 0) {
        notice_message note;
        note.known_blocks.mode = normal;
        note.known_blocks.pending = 0;
        enqueue(note);
        return;
    }
    std::unique_lock<std::mutex> g_conn( conn_mtx );
    if( last_handshake_recv.generation >= 1 ) {
        fc_dlog( logger, "maybe truncating branch at = ${h}:${id}",
                ("h", block_header::num_from_id(last_handshake_recv.head_id))("id", last_handshake_recv.head_id) );
    }

    block_id_type lib_id = last_handshake_recv.last_irreversible_block_id;
    g_conn.unlock();
    const auto lib_num = block_header::num_from_id(lib_id);
    if( lib_num == 0 ) return; // if last_irreversible_block_id is null (we have not received handshake or reset)

    // TODO: chain_plug
    // app().post( priority::medium, [chain_plug = my_impl->chain_plug, c = shared_from_this(),
    //       lib_num, head_num, msg_head_id]() {
    //    auto msg_head_num = block_header::num_from_id(msg_head_id);
    //    bool on_fork = msg_head_num == 0;
    //    bool unknown_block = false;
    //    if( !on_fork ) {
    //       try {
    //          const controller& cc = chain_plug->chain();
    //          block_id_type my_id = cc.get_block_id_for_num( msg_head_num );
    //          on_fork = my_id != msg_head_id;
    //       } catch( const unknown_block_exception& ) {
    //          unknown_block = true;
    //       } catch( ... ) {
    //          on_fork = true;
    //       }
    //    }
    //    if( unknown_block ) {
    //       c->strand.post( [msg_head_num, c]() {
    //          peer_ilog( c, "Peer asked for unknown block ${mn}, sending: benign_other go away", ("mn", msg_head_num) );
    //          c->no_retry = benign_other;
    //          c->enqueue( go_away_message( benign_other ) );
    //       } );
    //    } else {
    //       if( on_fork ) msg_head_num = 0;
    //       // if peer on fork, start at their last lib, otherwise we can start at msg_head+1
    //       c->strand.post( [c, msg_head_num, lib_num, head_num]() {
    //          c->blk_send_branch_impl( msg_head_num, lib_num, head_num );
    //       } );
    //    }
    // } );
}

void connection::blk_send_branch_impl( uint32_t msg_head_num, uint32_t lib_num, uint32_t head_num ) {
    if( !peer_requested ) {
        auto last = msg_head_num != 0 ? msg_head_num : lib_num;
        peer_requested = peer_sync_state( last+1, head_num, last );
    } else {
        auto last = msg_head_num != 0 ? msg_head_num : std::min( peer_requested->last, lib_num );
        uint32_t end   = std::max( peer_requested->end_block, head_num );
        peer_requested = peer_sync_state( last+1, end, last );
    }
    if( peer_requested->start_block <= peer_requested->end_block ) {
        fc_ilog( logger, "enqueue ${s} - ${e} to ${p}", ("s", peer_requested->start_block)("e", peer_requested->end_block)("p", peer_name()) );
        enqueue_sync_block();
    } else {
        fc_ilog( logger, "nothing to enqueue ${p} to ${p}", ("p", peer_name()) );
        peer_requested.reset();
    }
}

void connection::blk_send( const block_id_type& blkid ) {
    connection_wptr weak = shared_from_this();
    // TODO: chain_plug
    // app().post( priority::medium, [blkid, weak{std::move(weak)}]() {
    //    connection_ptr c = weak.lock();
    //    if( !c ) return;
    //    try {
    //       controller& cc = my_impl->chain_plug->chain();
    //       signed_block_ptr b = cc.fetch_block_by_id( blkid );
    //       if( b ) {
    //          fc_dlog( logger, "found block for id at num ${n}", ("n", b->block_num()) );
    //          my_impl->dispatcher->add_peer_block( blkid, c->connection_id );
    //          c->strand.post( [c, b{std::move(b)}]() {
    //             c->enqueue_block( b );
    //          } );
    //       } else {
    //          fc_ilog( logger, "fetch block by id returned null, id ${id} for ${p}",
    //                   ("id", blkid)( "p", c->peer_address() ) );
    //       }
    //    } catch( const assert_exception& ex ) {
    //       fc_elog( logger, "caught assert on fetch_block_by_id, ${ex}, id ${id} for ${p}",
    //                ("ex", ex.to_string())( "id", blkid )( "p", c->peer_address() ) );
    //    } catch( ... ) {
    //       fc_elog( logger, "caught other exception fetching block id ${id} for ${p}",
    //                ("id", blkid)( "p", c->peer_address() ) );
    //    }
    // });
}

void connection::stop_send() {
    syncing = false;
}

void connection::send_handshake( bool force ) {
    strand.post( [force, c = shared_from_this()]() {
        std::unique_lock<std::mutex> g_conn( c->conn_mtx );
        if( c->populate_handshake( c->last_handshake_sent, force ) ) {
        static_assert( std::is_same_v<decltype( c->sent_handshake_count ), int16_t>, "INT16_MAX based on int16_t" );
        if( c->sent_handshake_count == INT16_MAX ) c->sent_handshake_count = 1; // do not wrap
        c->last_handshake_sent.generation = ++c->sent_handshake_count;
        auto last_handshake_sent = c->last_handshake_sent;
        g_conn.unlock();
        fc_ilog( logger, "Sending handshake generation ${g} to ${ep}, lib ${lib}, head ${head}, id ${id}",
                    ("g", last_handshake_sent.generation)( "ep", c->peer_name())
                    ("lib", last_handshake_sent.last_irreversible_block_num)
                    ("head", last_handshake_sent.head_num)("id", last_handshake_sent.head_id.str().substr(8,16)) );
        c->enqueue( last_handshake_sent );
        }
    });
}

void connection::send_time() {
    time_message xpkt;
    xpkt.org = rec;
    xpkt.rec = dst;
    xpkt.xmt = get_time();
    org = xpkt.xmt;
    enqueue(xpkt);
}

void connection::send_time(const time_message& msg) {
    time_message xpkt;
    xpkt.org = msg.xmt;
    xpkt.rec = msg.dst;
    xpkt.xmt = get_time();
    enqueue(xpkt);
}

void connection::queue_write(const std::shared_ptr<vector<char>>& buff,
                            std::function<void(boost::system::error_code, std::size_t)> callback,
                            bool to_sync_queue) {
    if( !buffer_queue.add_write_queue( buff, callback, to_sync_queue )) {
        fc_wlog( logger, "write_queue full ${s} bytes, giving up on connection ${p}",
                ("s", buffer_queue.write_queue_size())("p", peer_name()) );
        close();
        return;
    }
    do_queue_write();
}

void connection::do_queue_write() {
    if( !buffer_queue.ready_to_send() )
        return;
    connection_ptr c(shared_from_this());

    std::vector<boost::asio::const_buffer> bufs;
    buffer_queue.fill_out_buffer( bufs );

    strand.post( [c{std::move(c)}, bufs{std::move(bufs)}]() {
        boost::asio::async_write( *c->socket, bufs,
        boost::asio::bind_executor( c->strand, [c, socket=c->socket]( boost::system::error_code ec, std::size_t w ) {
        try {
            c->buffer_queue.clear_out_queue();
            // May have closed connection and cleared buffer_queue
            if( !c->socket_is_open() || socket != c->socket ) {
                fc_ilog( logger, "async write socket ${r} before callback: ${p}",
                        ("r", c->socket_is_open() ? "changed" : "closed")("p", c->peer_name()) );
                c->close();
                return;
            }

            if( ec ) {
                if( ec.value() != boost::asio::error::eof ) {
                    fc_elog( logger, "Error sending to peer ${p}: ${i}", ("p", c->peer_name())( "i", ec.message() ) );
                } else {
                    fc_wlog( logger, "connection closure detected on write to ${p}", ("p", c->peer_name()) );
                }
                c->close();
                return;
            }

            c->buffer_queue.out_callback( ec, w );

            c->enqueue_sync_block();
            c->do_queue_write();
        } catch( const std::exception& ex ) {
            fc_elog( logger, "Exception in do_queue_write to ${p} ${s}", ("p", c->peer_name())( "s", ex.what() ) );
        } catch( const fc::exception& ex ) {
            fc_elog( logger, "Exception in do_queue_write to ${p} ${s}", ("p", c->peer_name())( "s", ex.to_string() ) );
        } catch( ... ) {
            fc_elog( logger, "Exception in do_queue_write to ${p}", ("p", c->peer_name()) );
        }
        }));
    });
}

void connection::cancel_sync(go_away_reason reason) {
    fc_dlog( logger, "cancel sync reason = ${m}, write queue size ${o} bytes peer ${p}",
            ("m", reason_str( reason ))( "o", buffer_queue.write_queue_size() )( "p", peer_address() ) );
    cancel_wait();
    flush_queues();
    switch (reason) {
    case validation :
    case fatal_other : {
        no_retry = reason;
        enqueue( go_away_message( reason ));
        break;
    }
    default:
        fc_ilog(logger, "sending empty request but not calling sync wait on ${p}", ("p",peer_address()));
        enqueue( ( sync_request_message ) {0,0} );
    }
}

bool connection::enqueue_sync_block() {
    if( !peer_requested ) {
        return false;
    } else {
        fc_dlog( logger, "enqueue sync block ${num}", ("num", peer_requested->last + 1) );
    }
    uint32_t num = ++peer_requested->last;
    if(num == peer_requested->end_block) {
        peer_requested.reset();
        fc_ilog( logger, "completing enqueue_sync_block ${num} to ${p}", ("num", num)("p", peer_name()) );
    }
    // TODO: chain_plug
    // connection_wptr weak = shared_from_this();
    // app().post( priority::medium, [num, weak{std::move(weak)}]() {
    //    connection_ptr c = weak.lock();
    //    if( !c ) return;
    //    controller& cc = my_impl->chain_plug->chain();
    //    signed_block_ptr sb;
    //    try {
    //       sb = cc.fetch_block_by_number( num );
    //    } FC_LOG_AND_DROP();
    //    if( sb ) {
    //       c->strand.post( [c, sb{std::move(sb)}]() {
    //          c->enqueue_block( sb, true );
    //       });
    //    } else {
    //       c->strand.post( [c, num]() {
    //          peer_ilog( c, "enqueue sync, unable to fetch block ${num}", ("num", num) );
    //          c->send_handshake();
    //       });
    //    }
    // });

    return true;
}

void connection::enqueue( const net_message& m ) {
    verify_strand_in_this_thread( strand, __func__, __LINE__ );
    go_away_reason close_after_send = no_reason;
    if (m.contains<go_away_message>()) {
        close_after_send = m.get<go_away_message>().reason;
    }

    const uint32_t payload_size = fc::raw::pack_size( m );

    const char* const header = reinterpret_cast<const char* const>(&payload_size); // avoid variable size encoding of uint32_t
    constexpr size_t header_size = sizeof(payload_size);
    static_assert( header_size == message_header_size, "invalid message_header_size" );
    const size_t buffer_size = header_size + payload_size;

    auto send_buffer = std::make_shared<vector<char>>(buffer_size);
    fc::datastream<char*> ds( send_buffer->data(), buffer_size);
    ds.write( header, header_size );
    fc::raw::pack( ds, m );

    enqueue_buffer( send_buffer, close_after_send );
}


void connection::enqueue_block( const signed_block_ptr& sb, bool to_sync_queue) {
    fc_dlog( logger, "enqueue block ${num}", ("num", sb->block_num()) );
    verify_strand_in_this_thread( strand, __func__, __LINE__ );
    enqueue_buffer( create_send_buffer( sb ), no_reason, to_sync_queue);
}

void connection::enqueue_buffer( const std::shared_ptr<std::vector<char>>& send_buffer,
                                go_away_reason close_after_send,
                                bool to_sync_queue)
{
    connection_ptr self = shared_from_this();
    queue_write(send_buffer,
        [conn{std::move(self)}, close_after_send](boost::system::error_code ec, std::size_t ) {
                    if (ec) return;
                    if (close_after_send != no_reason) {
                        fc_ilog( logger, "sent a go away message: ${r}, closing connection to ${p}",
                                ("r", reason_str(close_after_send))("p", conn->peer_name()) );
                        conn->close();
                        return;
                    }
                },
                to_sync_queue);
}

// thread safe
void connection::cancel_wait() {
    std::lock_guard<std::mutex> g( response_expected_timer_mtx );
    response_expected_timer.cancel();
}

// thread safe
void connection::sync_wait() {
    connection_ptr c(shared_from_this());
    std::lock_guard<std::mutex> g( response_expected_timer_mtx );
    response_expected_timer.expires_from_now( my_impl->resp_expected_period );
    response_expected_timer.async_wait(
        boost::asio::bind_executor( c->strand, [c]( boost::system::error_code ec ) {
            c->sync_timeout( ec );
        } ) );
}

// thread safe
void connection::fetch_wait() {
    connection_ptr c( shared_from_this() );
    std::lock_guard<std::mutex> g( response_expected_timer_mtx );
    response_expected_timer.expires_from_now( my_impl->resp_expected_period );
    response_expected_timer.async_wait(
        boost::asio::bind_executor( c->strand, [c]( boost::system::error_code ec ) {
            c->fetch_timeout(ec);
        } ) );
}

// called from connection strand
void connection::sync_timeout( boost::system::error_code ec ) {
    if( !ec ) {
        my_impl->sync_master->sync_reassign_fetch( shared_from_this(), benign_other );
    } else if( ec == boost::asio::error::operation_aborted ) {
    } else {
        fc_elog( logger, "setting timer for sync request got error ${ec}", ("ec", ec.message()) );
    }
}

// locks conn_mtx, do not call while holding conn_mtx
const string connection::peer_name() {
    std::lock_guard<std::mutex> g_conn( conn_mtx );
    if( !last_handshake_recv.p2p_address.empty() ) {
        return last_handshake_recv.p2p_address;
    }
    if( !peer_address().empty() ) {
        return peer_address();
    }
    if( remote_endpoint_port != unknown ) {
        return remote_endpoint_ip + ":" + remote_endpoint_port;
    }
    return "connecting client";
}

void connection::fetch_timeout( boost::system::error_code ec ) {
    if( !ec ) {
        my_impl->dispatcher->retry_fetch( shared_from_this() );
    } else if( ec == boost::asio::error::operation_aborted ) {
        if( !connected() ) {
        fc_dlog( logger, "fetch timeout was cancelled due to dead connection" );
        }
    } else {
        fc_elog( logger, "setting timer for fetch request got error ${ec}", ("ec", ec.message() ) );
    }
}

void connection::request_sync_blocks(uint32_t start, uint32_t end) {
    sync_request_message srm = {start,end};
    enqueue( net_message(srm) );
    sync_wait();
}


//------------------------------------------------------------------------

// called from any thread
bool connection::resolve_and_connect() {
    switch ( no_retry ) {
        case no_reason:
        case wrong_version:
        case benign_other:
        break;
        default:
        fc_dlog( logger, "Skipping connect due to go_away reason ${r}",("r", reason_str( no_retry )));
        return false;
    }

    string::size_type colon = peer_address().find(':');
    if (colon == std::string::npos || colon == 0) {
        fc_elog( logger, "Invalid peer address. must be \"host:port[:<blk>|<trx>]\": ${p}", ("p", peer_address()) );
        return false;
    }

    connection_ptr c = shared_from_this();

    if( consecutive_immediate_connection_close > def_max_consecutive_immediate_connection_close || no_retry == benign_other ) {
        auto connector_period_us = std::chrono::duration_cast<std::chrono::microseconds>( my_impl->connector_period );
        std::lock_guard<std::mutex> g( c->conn_mtx );
        if( last_close == fc::time_point() || last_close > fc::time_point::now() - fc::microseconds( connector_period_us.count() ) ) {
        return true; // true so doesn't remove from valid connections
        }
    }

    strand.post([c]() {
        string::size_type colon = c->peer_address().find(':');
        string::size_type colon2 = c->peer_address().find(':', colon + 1);
        string host = c->peer_address().substr( 0, colon );
        string port = c->peer_address().substr( colon + 1, colon2 == string::npos ? string::npos : colon2 - (colon + 1));
        idump((host)(port));
        c->set_connection_type( c->peer_address() );
        tcp::resolver::query query( tcp::v4(), host, port );
        // Note: need to add support for IPv6 too

        auto resolver = std::make_shared<tcp::resolver>( my_impl->thread_pool->get_executor() );
        connection_wptr weak_conn = c;
        resolver->async_resolve( query, boost::asio::bind_executor( c->strand,
        [resolver, weak_conn]( const boost::system::error_code& err, tcp::resolver::results_type endpoints ) {
            auto c = weak_conn.lock();
            if( !c ) return;
            if( !err ) {
                c->connect( resolver, endpoints );
            } else {
                fc_elog( logger, "Unable to resolve ${add}: ${error}", ("add", c->peer_name())( "error", err.message() ) );
                c->connecting = false;
                ++c->consecutive_immediate_connection_close;
            }
        } ) );
    } );
    return true;
}

// called from connection strand
void connection::connect( const std::shared_ptr<tcp::resolver>& resolver, tcp::resolver::results_type endpoints ) {
    switch ( no_retry ) {
        case no_reason:
        case wrong_version:
        case benign_other:
        break;
        default:
        return;
    }
    connecting = true;
    pending_message_buffer.reset();
    buffer_queue.clear_out_queue();
    boost::asio::async_connect( *socket, endpoints,
        boost::asio::bind_executor( strand,
            [resolver, c = shared_from_this(), socket=socket]( const boost::system::error_code& err, const tcp::endpoint& endpoint ) {
        if( !err && socket->is_open() && socket == c->socket ) {
            if( c->start_session() ) {
                c->send_handshake();
            }
        } else {
            fc_elog( logger, "connection failed to ${peer}: ${error}", ("peer", c->peer_name())( "error", err.message()));
            c->close( false );
        }
    } ) );
}


// only called from strand thread
void connection::start_read_message() {
    try {
        std::size_t minimum_read =
            std::atomic_exchange<decltype(outstanding_read_bytes.load())>( &outstanding_read_bytes, 0 );
        minimum_read = minimum_read != 0 ? minimum_read : message_header_size;

        if (my_impl->use_socket_read_watermark) {
        const size_t max_socket_read_watermark = 4096;
        std::size_t socket_read_watermark = std::min<std::size_t>(minimum_read, max_socket_read_watermark);
        boost::asio::socket_base::receive_low_watermark read_watermark_opt(socket_read_watermark);
        boost::system::error_code ec;
        socket->set_option( read_watermark_opt, ec );
        if( ec ) {
            fc_elog( logger, "unable to set read watermark ${peer}: ${e1}", ("peer", peer_name())( "e1", ec.message() ) );
        }
        }

        auto completion_handler = [minimum_read](boost::system::error_code ec, std::size_t bytes_transferred) -> std::size_t {
        if (ec || bytes_transferred >= minimum_read ) {
            return 0;
        } else {
            return minimum_read - bytes_transferred;
        }
        };

        uint32_t write_queue_size = buffer_queue.write_queue_size();
        if( write_queue_size > def_max_write_queue_size ) {
        fc_elog( logger, "write queue full ${s} bytes, giving up on connection, closing connection to: ${p}",
                    ("s", write_queue_size)("p", peer_name()) );
        close( false );
        return;
        }

        boost::asio::async_read( *socket,
        pending_message_buffer.get_buffer_sequence_for_boost_async_read(), completion_handler,
        boost::asio::bind_executor( strand,
            [conn = shared_from_this(), socket=socket]( boost::system::error_code ec, std::size_t bytes_transferred ) {
            // may have closed connection and cleared pending_message_buffer
            if( !conn->socket_is_open() || socket != conn->socket ) return;

            bool close_connection = false;
            try {
                if( !ec ) {
                    if (bytes_transferred > conn->pending_message_buffer.bytes_to_write()) {
                    fc_elog( logger,"async_read_some callback: bytes_transfered = ${bt}, buffer.bytes_to_write = ${btw}",
                                ("bt",bytes_transferred)("btw",conn->pending_message_buffer.bytes_to_write()) );
                    }
                    EOS_ASSERT(bytes_transferred <= conn->pending_message_buffer.bytes_to_write(), plugin_exception, "");
                    conn->pending_message_buffer.advance_write_ptr(bytes_transferred);
                    while (conn->pending_message_buffer.bytes_to_read() > 0) {
                    uint32_t bytes_in_buffer = conn->pending_message_buffer.bytes_to_read();

                    if (bytes_in_buffer < message_header_size) {
                        conn->outstanding_read_bytes = message_header_size - bytes_in_buffer;
                        break;
                    } else {
                        uint32_t message_length;
                        auto index = conn->pending_message_buffer.read_index();
                        conn->pending_message_buffer.peek(&message_length, sizeof(message_length), index);
                        if(message_length > def_send_buffer_size*2 || message_length == 0) {
                            fc_elog( logger,"incoming message length unexpected (${i})", ("i", message_length) );
                            close_connection = true;
                            break;
                        }

                        auto total_message_bytes = message_length + message_header_size;

                        if (bytes_in_buffer >= total_message_bytes) {
                            conn->pending_message_buffer.advance_read_ptr(message_header_size);
                            conn->consecutive_immediate_connection_close = 0;
                            if (!conn->process_next_message(message_length)) {
                                return;
                            }
                        } else {
                            auto outstanding_message_bytes = total_message_bytes - bytes_in_buffer;
                            auto available_buffer_bytes = conn->pending_message_buffer.bytes_to_write();
                            if (outstanding_message_bytes > available_buffer_bytes) {
                                conn->pending_message_buffer.add_space( outstanding_message_bytes - available_buffer_bytes );
                            }

                            conn->outstanding_read_bytes = outstanding_message_bytes;
                            break;
                        }
                    }
                    }
                    if( !close_connection ) conn->start_read_message();
                } else {
                    if (ec.value() != boost::asio::error::eof) {
                    fc_elog( logger, "Error reading message: ${m}", ( "m", ec.message() ) );
                    } else {
                    fc_ilog( logger, "Peer closed connection" );
                    }
                    close_connection = true;
                }
            }
            catch(const std::exception &ex) {
                fc_elog( logger, "Exception in handling read data: ${s}", ("s",ex.what()) );
                close_connection = true;
            }
            catch(const fc::exception &ex) {
                fc_elog( logger, "Exception in handling read data ${s}", ("s",ex.to_string()) );
                close_connection = true;
            }
            catch (...) {
                fc_elog( logger, "Undefined exception handling read data" );
                close_connection = true;
            }

            if( close_connection ) {
                fc_elog( logger, "Closing connection to: ${p}", ("p", conn->peer_name()) );
                conn->close();
            }
        }));
    } catch (...) {
        fc_elog( logger, "Undefined exception in start_read_message, closing connection to: ${p}", ("p", peer_name()) );
        close();
    }
}

// called from connection strand
bool connection::process_next_message( uint32_t message_length ) {
    try {
        // if next message is a block we already have, exit early
        auto peek_ds = pending_message_buffer.create_peek_datastream();
        unsigned_int which{};
        fc::raw::unpack( peek_ds, which );
        if( which == signed_block_which ) {
        block_header bh;
        fc::raw::unpack( peek_ds, bh );

        const block_id_type blk_id = bh.id();
        const uint32_t blk_num = bh.block_num();
        if( my_impl->dispatcher->have_block( blk_id ) ) {
            fc_dlog( logger, "canceling wait on ${p}, already received block ${num}, id ${id}...",
                    ("p", peer_name())("num", blk_num)("id", blk_id.str().substr(8,16)) );
            my_impl->sync_master->sync_recv_block( shared_from_this(), blk_id, blk_num, false );
            cancel_wait();

            pending_message_buffer.advance_read_ptr( message_length );
            return true;
        }
        fc_dlog( logger, "${p} received block ${num}, id ${id}..., latency: ${latency}",
                    ("p", peer_name())("num", bh.block_num())("id", blk_id.str().substr(8,16))
                    ("latency", (fc::time_point::now() - bh.timestamp).count()/1000) );
        if( !my_impl->sync_master->syncing_with_peer() ) { // guard against peer thinking it needs to send us old blocks
            uint32_t lib = 0;
            std::tie( lib, std::ignore, std::ignore, std::ignore, std::ignore, std::ignore ) = my_impl->get_chain_info();
            if( blk_num < lib ) {
                std::unique_lock<std::mutex> g( conn_mtx );
                const auto last_sent_lib = last_handshake_sent.last_irreversible_block_num;
                g.unlock();
                if( blk_num < last_sent_lib ) {
                    fc_ilog( logger, "received block ${n} less than sent lib ${lib}", ("n", blk_num)("lib", last_sent_lib) );
                    close();
                } else {
                    fc_ilog( logger, "received block ${n} less than lib ${lib}", ("n", blk_num)("lib", lib) );
                    enqueue( (sync_request_message) {0, 0} );
                    send_handshake();
                    cancel_wait();
                }

                pending_message_buffer.advance_read_ptr( message_length );
                return true;
            }
        }

        auto ds = pending_message_buffer.create_datastream();
        fc::raw::unpack( ds, which ); // throw away
        shared_ptr<signed_block> ptr = std::make_shared<signed_block>();
        fc::raw::unpack( ds, *ptr );

        auto is_webauthn_sig = []( const fc::crypto::signature& s ) {
            return s.which() == fc::crypto::signature::storage_type::position<fc::crypto::webauthn::signature>();
        };
        bool has_webauthn_sig = is_webauthn_sig( ptr->producer_signature );

        constexpr auto additional_sigs_eid = additional_block_signatures_extension::extension_id();
        auto exts = ptr->validate_and_extract_extensions();
        if( exts.count( additional_sigs_eid ) ) {
            const auto &additional_sigs = exts.lower_bound( additional_sigs_eid )->second.get<additional_block_signatures_extension>().signatures;
            has_webauthn_sig |= std::any_of( additional_sigs.begin(), additional_sigs.end(), is_webauthn_sig );
        }

        if( has_webauthn_sig ) {
            fc_dlog( logger, "WebAuthn signed block received from ${p}, closing connection", ("p", peer_name()));
            close();
            return false;
        }

        handle_message( blk_id, std::move( ptr ) );

        } else if( which == packed_transaction_which ) {
        if( !my_impl->p2p_accept_transactions ) {
            fc_dlog( logger, "p2p-accept-transaction=false - dropping txn" );
            pending_message_buffer.advance_read_ptr( message_length );
            return true;
        }

        auto ds = pending_message_buffer.create_datastream();
        fc::raw::unpack( ds, which ); // throw away
        shared_ptr<packed_transaction> ptr = std::make_shared<packed_transaction>();
        fc::raw::unpack( ds, *ptr );
        handle_message( std::move( ptr ) );

        } else {
        auto ds = pending_message_buffer.create_datastream();
        net_message msg;
        fc::raw::unpack( ds, msg );
        msg_handler m( shared_from_this() );
        msg.visit( m );
        }

    } catch( const fc::exception& e ) {
        fc_elog( logger, "Exception in handling message from ${p}: ${s}",
                ("p", peer_name())("s", e.to_detail_string()) );
        close();
        return false;
    }
    return true;
}


bool connection::is_valid( const handshake_message& msg ) {
    // Do some basic validation of an incoming handshake_message, so things
    // that really aren't handshake messages can be quickly discarded without
    // affecting state.
    bool valid = true;
    if (msg.last_irreversible_block_num > msg.head_num) {
        fc_wlog( logger, "Handshake message validation: last irreversible block (${i}) is greater than head block (${h})",
                ("i", msg.last_irreversible_block_num)("h", msg.head_num) );
        valid = false;
    }
    if (msg.p2p_address.empty()) {
        fc_wlog( logger, "Handshake message validation: p2p_address is null string" );
        valid = false;
    } else if( msg.p2p_address.length() > max_handshake_str_length ) {
        // see max_handshake_str_length comment in protocol.hpp
        fc_wlog( logger, "Handshake message validation: p2p_address to large: ${p}", ("p", msg.p2p_address.substr(0, max_handshake_str_length) + "...") );
        valid = false;
    }
    if (msg.os.empty()) {
        fc_wlog( logger, "Handshake message validation: os field is null string" );
        valid = false;
    } else if( msg.os.length() > max_handshake_str_length ) {
        fc_wlog( logger, "Handshake message validation: os field to large: ${p}", ("p", msg.os.substr(0, max_handshake_str_length) + "...") );
        valid = false;
    }
    if( msg.agent.length() > max_handshake_str_length ) {
        fc_wlog( logger, "Handshake message validation: agent field to large: ${p}", ("p", msg.agent.substr(0, max_handshake_str_length) + "...") );
        valid = false;
    }
    if ((msg.sig != chain::signature_type() || msg.token != fc::sha256()) && (msg.token != fc::sha256::hash(msg.time))) {
        fc_wlog( logger, "Handshake message validation: token field invalid" );
        valid = false;
    }
    return valid;
}

void connection::handle_message( const chain_size_message& msg ) {
    peer_dlog(this, "received chain_size_message");
}

void connection::handle_message( const handshake_message& msg ) {
    peer_dlog( this, "received handshake_message" );
    if( !is_valid( msg ) ) {
        peer_elog( this, "bad handshake message");
        enqueue( go_away_message( fatal_other ) );
        return;
    }
    fc_dlog( logger, "received handshake gen ${g} from ${ep}, lib ${lib}, head ${head}",
            ("g", msg.generation)( "ep", peer_name() )
            ( "lib", msg.last_irreversible_block_num )( "head", msg.head_num ) );

    connecting = false;
    if (msg.generation == 1) {
        if( msg.node_id == my_impl->node_id) {
        fc_elog( logger, "Self connection detected node_id ${id}. Closing connection", ("id", msg.node_id) );
        enqueue( go_away_message( self ) );
        return;
        }

        if( peer_address().empty() ) {
        set_connection_type( msg.p2p_address );
        }

        std::unique_lock<std::mutex> g_conn( conn_mtx );
        if( peer_address().empty() || last_handshake_recv.node_id == fc::sha256()) {
        g_conn.unlock();
        fc_dlog(logger, "checking for duplicate" );
        std::shared_lock<std::shared_mutex> g_cnts( my_impl->connections_mtx );
        for(const auto& check : my_impl->connections) {
            if(check.get() == this)
                continue;
            if(check->connected() && check->peer_name() == msg.p2p_address) {
                // It's possible that both peers could arrive here at relatively the same time, so
                // we need to avoid the case where they would both tell a different connection to go away.
                // Using the sum of the initial handshake times of the two connections, we will
                // arbitrarily (but consistently between the two peers) keep one of them.
                std::unique_lock<std::mutex> g_check_conn( check->conn_mtx );
                auto check_time = check->last_handshake_sent.time + check->last_handshake_recv.time;
                g_check_conn.unlock();
                g_conn.lock();
                auto c_time = last_handshake_sent.time;
                g_conn.unlock();
                if (msg.time + c_time <= check_time)
                    continue;

                g_cnts.unlock();
                fc_dlog( logger, "sending go_away duplicate to ${ep}", ("ep",msg.p2p_address) );
                go_away_message gam(duplicate);
                g_conn.lock();
                gam.node_id = conn_node_id;
                g_conn.unlock();
                enqueue(gam);
                no_retry = duplicate;
                return;
            }
        }
        } else {
        fc_dlog( logger, "skipping duplicate check, addr == ${pa}, id = ${ni}",
                    ("pa", peer_address())( "ni", last_handshake_recv.node_id ) );
        g_conn.unlock();
        }

        if( msg.chain_id != my_impl->chain_id ) {
        fc_elog( logger, "Peer on a different chain. Closing connection" );
        enqueue( go_away_message(go_away_reason::wrong_chain) );
        return;
        }
        protocol_version = my_impl->to_protocol_version(msg.network_version);
        if( protocol_version != net_version ) {
        fc_ilog( logger, "Local network version: ${nv} Remote version: ${mnv}",
                    ("nv", net_version)( "mnv", protocol_version ) );
        }

        g_conn.lock();
        if( conn_node_id != msg.node_id ) {
        conn_node_id = msg.node_id;
        }
        g_conn.unlock();

        if( !my_impl->authenticate_peer( msg ) ) {
        fc_elog( logger, "Peer not authenticated.  Closing connection." );
        enqueue( go_away_message( authentication ) );
        return;
        }

        uint32_t peer_lib = msg.last_irreversible_block_num;

        // TODO: chain_plug
        // connection_wptr weak = shared_from_this();
        // app().post( priority::medium, [peer_lib, chain_plug = my_impl->chain_plug, weak{std::move(weak)},
        //                             msg_lib_id = msg.last_irreversible_block_id]() {
        //    connection_ptr c = weak.lock();
        //    if( !c ) return;
        //    controller& cc = chain_plug->chain();
        //    uint32_t lib_num = cc.last_irreversible_block_num();

        //    fc_dlog( logger, "handshake, check for fork lib_num = ${ln} peer_lib = ${pl}", ("ln", lib_num)( "pl", peer_lib ) );

        //    if( peer_lib <= lib_num && peer_lib > 0 ) {
        //       bool on_fork = false;
        //       try {
        //          block_id_type peer_lib_id = cc.get_block_id_for_num( peer_lib );
        //          on_fork = (msg_lib_id != peer_lib_id);
        //       } catch( const unknown_block_exception& ) {
        //          // allow this for now, will be checked on sync
        //          peer_dlog( c, "peer last irreversible block ${pl} is unknown", ("pl", peer_lib) );
        //       } catch( ... ) {
        //          peer_wlog( c, "caught an exception getting block id for ${pl}", ("pl", peer_lib) );
        //          on_fork = true;
        //       }
        //       if( on_fork ) {
        //          c->strand.post( [c]() {
        //             peer_elog( c, "Peer chain is forked, sending: forked go away" );
        //             c->enqueue( go_away_message( forked ) );
        //          } );
        //       }
        //    }
        // });

        if( sent_handshake_count == 0 ) {
        send_handshake();
        }
    }

    std::unique_lock<std::mutex> g_conn( conn_mtx );
    last_handshake_recv = msg;
    g_conn.unlock();
    my_impl->sync_master->recv_handshake( shared_from_this(), msg );
}

void connection::handle_message( const go_away_message& msg ) {
    peer_wlog( this, "received go_away_message, reason = ${r}", ("r", reason_str( msg.reason )) );
    bool retry = no_retry == no_reason; // if no previous go away message
    no_retry = msg.reason;
    if( msg.reason == duplicate ) {
        std::lock_guard<std::mutex> g_conn( conn_mtx );
        conn_node_id = msg.node_id;
    }
    if( msg.reason == wrong_version ) {
        if( !retry ) no_retry = fatal_other; // only retry once on wrong version
    } else {
        retry = false;
    }
    flush_queues();
    close( retry ); // reconnect if wrong_version
}

void connection::handle_message( const time_message& msg ) {
    peer_dlog( this, "received time_message" );
    /* We've already lost however many microseconds it took to dispatch
    * the message, but it can't be helped.
    */
    msg.dst = get_time();

    // If the transmit timestamp is zero, the peer is horribly broken.
    if(msg.xmt == 0)
        return;                 /* invalid timestamp */

    if(msg.xmt == xmt)
        return;                 /* duplicate packet */

    xmt = msg.xmt;
    rec = msg.rec;
    dst = msg.dst;

    if( msg.org == 0 ) {
        send_time( msg );
        return;  // We don't have enough data to perform the calculation yet.
    }

    double offset = (double(rec - org) + double(msg.xmt - dst)) / 2;
    double NsecPerUsec{1000};

    if( logger.is_enabled( fc::log_level::all ) )
        logger.log( FC_LOG_MESSAGE( all, "Clock offset is ${o}ns (${us}us)",
                                    ("o", offset)( "us", offset / NsecPerUsec ) ) );
    org = 0;
    rec = 0;

    std::unique_lock<std::mutex> g_conn( conn_mtx );
    if( last_handshake_recv.generation == 0 ) {
        g_conn.unlock();
        send_handshake();
    }
}

void connection::handle_message( const notice_message& msg ) {
    // peer tells us about one or more blocks or txns. When done syncing, forward on
    // notices of previously unknown blocks or txns,
    //
    peer_dlog( this, "received notice_message" );
    connecting = false;
    if( msg.known_blocks.ids.size() > 1 ) {
        fc_elog( logger, "Invalid notice_message, known_blocks.ids.size ${s}, closing connection: ${p}",
                ("s", msg.known_blocks.ids.size())("p", peer_address()) );
        close( false );
        return;
    }
    if( msg.known_trx.mode != none ) {
        if( logger.is_enabled( fc::log_level::debug ) ) {
        const block_id_type& blkid = msg.known_blocks.ids.empty() ? block_id_type{} : msg.known_blocks.ids.back();
        fc_dlog( logger, "this is a ${m} notice with ${n} pending blocks: ${num} ${id}...",
                    ("m", modes_str( msg.known_blocks.mode ))("n", msg.known_blocks.pending)
                    ("num", block_header::num_from_id( blkid ))("id", blkid.str().substr( 8, 16 )) );
        }
    }
    switch (msg.known_trx.mode) {
    case none:
        break;
    case last_irr_catch_up: {
        std::unique_lock<std::mutex> g_conn( conn_mtx );
        last_handshake_recv.head_num = msg.known_blocks.pending;
        g_conn.unlock();
        break;
    }
    case catch_up : {
        break;
    }
    case normal: {
        my_impl->dispatcher->recv_notice( shared_from_this(), msg, false );
    }
    }

    if( msg.known_blocks.mode != none ) {
        fc_dlog( logger, "this is a ${m} notice with ${n} blocks",
                ("m", modes_str( msg.known_blocks.mode ))( "n", msg.known_blocks.pending ) );
    }
    switch (msg.known_blocks.mode) {
    case none : {
        break;
    }
    case last_irr_catch_up:
    case catch_up: {
        my_impl->sync_master->sync_recv_notice( shared_from_this(), msg );
        break;
    }
    case normal : {
        my_impl->dispatcher->recv_notice( shared_from_this(), msg, false );
        break;
    }
    default: {
        peer_elog( this, "bad notice_message : invalid known_blocks.mode ${m}",
                ("m", static_cast<uint32_t>(msg.known_blocks.mode)) );
    }
    }
}

void connection::handle_message( const request_message& msg ) {
    if( msg.req_blocks.ids.size() > 1 ) {
        fc_elog( logger, "Invalid request_message, req_blocks.ids.size ${s}, closing ${p}",
                ("s", msg.req_blocks.ids.size())( "p", peer_name() ) );
        close();
        return;
    }

    switch (msg.req_blocks.mode) {
    case catch_up :
        peer_dlog( this, "received request_message:catch_up" );
        blk_send_branch( msg.req_blocks.ids.empty() ? block_id_type() : msg.req_blocks.ids.back() );
        break;
    case normal :
        peer_dlog( this, "received request_message:normal" );
        if( !msg.req_blocks.ids.empty() ) {
        blk_send( msg.req_blocks.ids.back() );
        }
        break;
    default:;
    }


    switch (msg.req_trx.mode) {
    case catch_up :
        break;
    case none :
        if( msg.req_blocks.mode == none ) {
        stop_send();
        }
        // no break
    case normal :
        if( !msg.req_trx.ids.empty() ) {
        fc_elog( logger, "Invalid request_message, req_trx.ids.size ${s}", ("s", msg.req_trx.ids.size()) );
        close();
        return;
        }
        break;
    default:;
    }
}

void connection::handle_message( const sync_request_message& msg ) {
    fc_dlog( logger, "peer requested ${start} to ${end}", ("start", msg.start_block)("end", msg.end_block) );
    if( msg.end_block == 0 ) {
        peer_requested.reset();
        flush_queues();
    } else {
        peer_requested = peer_sync_state( msg.start_block, msg.end_block, msg.start_block-1);
        enqueue_sync_block();
    }
}

size_t calc_trx_size( const packed_transaction_ptr& trx ) {
    // transaction is stored packed and unpacked, double packed_size and size of signed as an approximation of use
    return (trx->get_packed_transaction().size() * 2 + sizeof(trx->get_signed_transaction())) * 2 +
            trx->get_packed_context_free_data().size() * 4 +
            trx->get_signatures().size() * sizeof(signature_type);
}

void connection::handle_message( packed_transaction_ptr trx ) {
    const auto& tid = trx->id();
    peer_dlog( this, "received packed_transaction ${id}", ("id", tid) );

    uint32_t trx_in_progress_sz = this->trx_in_progress_size.load();
    if( trx_in_progress_sz > def_max_trx_in_progress_size ) {
        char reason[72];
        snprintf(reason, 72, "Dropping trx, too many trx in progress %lu bytes", (unsigned long) trx_in_progress_sz);
        // TODO:
        // my_impl->producer_plug->log_failed_transaction(tid, reason);
        return;
    }

    bool have_trx = my_impl->dispatcher->have_txn( tid );
    node_transaction_state nts = {tid, trx->expiration(), 0, connection_id};
    my_impl->dispatcher->add_peer_txn( nts );

    if( have_trx ) {
        fc_dlog( logger, "got a duplicate transaction - dropping ${id}", ("id", tid) );
        return;
    }

    trx_in_progress_size += calc_trx_size( trx );

    // TODO: chain_plug
    // app().post( priority::low, [trx{std::move(trx)}, weak = weak_from_this()]() {
    //    my_impl->chain_plug->accept_transaction( trx,
    //       [weak, trx](const static_variant<fc::exception_ptr, transaction_trace_ptr>& result) mutable {
    //    // next (this lambda) called from application thread
    //    if (result.contains<fc::exception_ptr>()) {
    //       fc_dlog( logger, "bad packed_transaction : ${m}", ("m", result.get<fc::exception_ptr>()->what()) );
    //    } else {
    //       const transaction_trace_ptr& trace = result.get<transaction_trace_ptr>();
    //       if( !trace->except ) {
    //          fc_dlog( logger, "chain accepted transaction, bcast ${id}", ("id", trace->id) );
    //       } else {
    //          fc_elog( logger, "bad packed_transaction : ${m}", ("m", trace->except->what()));
    //       }
    //    }
    //    connection_ptr conn = weak.lock();
    //    if( conn ) {
    //       conn->trx_in_progress_size -= calc_trx_size( trx );
    //    }
    //   });
    // });
}

// called from connection strand
void connection::handle_message( const block_id_type& id, signed_block_ptr ptr ) {
    peer_dlog( this, "received signed_block ${id}", ("id", ptr->block_num() ) );
    auto priority = my_impl->sync_master->syncing_with_peer() ? priority::medium : priority::high;
    app().post(priority, [ptr{std::move(ptr)}, id, c = shared_from_this()]() mutable {
        c->process_signed_block( id, std::move( ptr ) );
    });
}

// called from application thread
void connection::process_signed_block( const block_id_type& blk_id, signed_block_ptr msg ) {

    // TODO: chain_plug
    // controller& cc = my_impl->chain_plug->chain();
    // uint32_t blk_num = msg->block_num();
    // // use c in this method instead of this to highlight that all methods called on c-> must be thread safe
    // connection_ptr c = shared_from_this();

    // // if we have closed connection then stop processing
    // if( !c->socket_is_open() )
    //    return;

    // try {
    //    if( cc.fetch_block_by_id(blk_id) ) {
    //       c->strand.post( [sync_master = my_impl->sync_master.get(),
    //                        dispatcher = my_impl->dispatcher.get(), c, blk_id, blk_num]() {
    //          dispatcher->add_peer_block( blk_id, c->connection_id );
    //          sync_master->sync_recv_block( c, blk_id, blk_num, false );
    //       });
    //       return;
    //    }
    // } catch( ...) {
    //    // should this even be caught?
    //    fc_elog( logger,"Caught an unknown exception trying to recall blockID" );
    // }

    // fc::microseconds age( fc::time_point::now() - msg->timestamp);
    // peer_dlog( c, "received signed_block : #${n} block age in secs = ${age}",
    //            ("n", blk_num)( "age", age.to_seconds() ) );

    // go_away_reason reason = fatal_other;
    // try {
    //    bool accepted = my_impl->chain_plug->accept_block(msg, blk_id);
    //    my_impl->update_chain_info();
    //    if( !accepted ) return;
    //    reason = no_reason;
    // } catch( const unlinkable_block_exception &ex) {
    //    peer_elog(c, "unlinkable_block_exception #${n} ${id}...: ${m}", ("n", blk_num)("id", blk_id.str().substr(8,16))("m",ex.to_string()));
    //    reason = unlinkable;
    // } catch( const block_validate_exception &ex) {
    //    peer_elog(c, "block_validate_exception #${n} ${id}...: ${m}", ("n", blk_num)("id", blk_id.str().substr(8,16))("m",ex.to_string()));
    //    reason = validation;
    // } catch( const assert_exception &ex) {
    //    peer_elog(c, "block assert_exception #${n} ${id}...: ${m}", ("n", blk_num)("id", blk_id.str().substr(8,16))("m",ex.to_string()));
    // } catch( const fc::exception &ex) {
    //    peer_elog(c, "bad block exception #${n} ${id}...: ${m}", ("n", blk_num)("id", blk_id.str().substr(8,16))("m",ex.to_string()));
    // } catch( ...) {
    //    peer_elog(c, "bad block #${n} ${id}...: unknown exception", ("n", blk_num)("id", blk_id.str().substr(8,16)));
    // }

    // if( reason == no_reason ) {
    //    boost::asio::post( my_impl->thread_pool->get_executor(), [dispatcher = my_impl->dispatcher.get(), cid=c->connection_id, blk_id, msg]() {
    //       fc_dlog( logger, "accepted signed_block : #${n} ${id}...", ("n", msg->block_num())("id", blk_id.str().substr(8,16)) );
    //       dispatcher->add_peer_block( blk_id, cid );
    //       dispatcher->update_txns_block_num( msg );
    //    });
    //    c->strand.post( [sync_master = my_impl->sync_master.get(), dispatcher = my_impl->dispatcher.get(), c, blk_id, blk_num]() {
    //       dispatcher->recv_block( c, blk_id, blk_num );
    //       sync_master->sync_recv_block( c, blk_id, blk_num, true );
    //    });
    // } else {
    //    c->strand.post( [sync_master = my_impl->sync_master.get(), dispatcher = my_impl->dispatcher.get(), c, blk_id, blk_num]() {
    //       sync_master->rejected_block( c, blk_num );
    //       dispatcher->rejected_block( blk_id );
    //    });
    // }
}


// call from connection strand
bool connection::populate_handshake( handshake_message& hello, bool force ) {
    namespace sc = std::chrono;
    bool send = force;
    hello.network_version = net_version_base + net_version;
    const auto prev_head_id = hello.head_id;
    uint32_t lib, head;
    std::tie( lib, std::ignore, head,
            hello.last_irreversible_block_id, std::ignore, hello.head_id ) = my_impl->get_chain_info();
    // only send handshake if state has changed since last handshake
    send |= lib != hello.last_irreversible_block_num;
    send |= head != hello.head_num;
    send |= prev_head_id != hello.head_id;
    if( !send ) return false;
    hello.last_irreversible_block_num = lib;
    hello.head_num = head;
    hello.chain_id = my_impl->chain_id;
    hello.node_id = my_impl->node_id;
    hello.key = my_impl->get_authentication_key();
    hello.time = sc::duration_cast<sc::nanoseconds>(sc::system_clock::now().time_since_epoch()).count();
    hello.token = fc::sha256::hash(hello.time);
    hello.sig = my_impl->sign_compact(hello.key, hello.token);
    // If we couldn't sign, don't send a token.
    if(hello.sig == chain::signature_type())
        hello.token = fc::sha256();
    hello.p2p_address = my_impl->p2p_address;
    if( is_transactions_only_connection() ) hello.p2p_address += ":trx";
    if( is_blocks_only_connection() ) hello.p2p_address += ":blk";
    hello.p2p_address += " - " + hello.node_id.str().substr(0,7);
#if defined( __APPLE__ )
    hello.os = "osx";
#elif defined( __linux__ )
    hello.os = "linux";
#elif defined( _WIN32 )
    hello.os = "win32";
#else
    hello.os = "other";
#endif
    hello.agent = my_impl->user_agent_name;

    return true;
}

} // namespace eosio