// Copyright (c) 2009-2010 Satoshi Nakamoto
// Copyright (c) 2020-2021 The nchain Developers
// Distributed under the MIT/X11 software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#pragma once

#include <eosio/chain/exceptions.hpp>
#include <eosio/net_plugin/protocol.hpp>
#include <eosio/net_plugin/connection_base.hpp>
#include <eosio/chain/block.hpp>
#include <eosio/chain/thread_utils.hpp>
#include <eosio/chain/contract_types.hpp>
#include <eosio/chain/generated_transaction_object.hpp>

#include <fc/network/message_buffer.hpp>
#include <fc/network/ip.hpp>
#include <fc/io/json.hpp>
#include <fc/io/raw.hpp>
#include <fc/log/appender.hpp>
#include <fc/log/logger_config.hpp>
#include <fc/reflect/variant.hpp>
#include <fc/crypto/rand.hpp>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ip/host_name.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/io_context_strand.hpp>

#include <atomic>
#include <shared_mutex>
#include <memory>

namespace eosio {

using boost::asio::ip::tcp;
using boost::asio::ip::address_v4;
using boost::asio::ip::host_name;
using boost::multi_index_container;
using strand_t = boost::asio::io_context::strand;

/**
* default value initializers
*/
constexpr auto     def_send_buffer_size_mb = 4;
constexpr auto     def_send_buffer_size = 1024*1024*def_send_buffer_size_mb;
constexpr auto     def_max_write_queue_size = def_send_buffer_size*10;
constexpr auto     def_max_trx_in_progress_size = 100*1024*1024; // 100 MB
constexpr auto     def_max_consecutive_rejected_blocks = 13; // num of rejected blocks before disconnect
constexpr auto     def_max_consecutive_immediate_connection_close = 9; // back off if client keeps closing
constexpr auto     def_max_clients = 25; // 0 for unlimited clients
constexpr auto     def_max_nodes_per_host = 1;
constexpr auto     def_conn_retry_wait = 30;
constexpr auto     def_txn_expire_wait = std::chrono::seconds(3);
constexpr auto     def_resp_expected_wait = std::chrono::seconds(5);
constexpr auto     def_sync_fetch_span = 100;

constexpr auto     message_header_size = 4;
constexpr uint32_t signed_block_which = 7;        // see protocol net_message
constexpr uint32_t packed_transaction_which = 8;  // see protocol net_message

/**
*  If there is a change to network protocol or behavior, increment net version to identify
*  the need for compatibility hooks
*/
constexpr uint16_t proto_base = 0;
constexpr uint16_t proto_explicit_sync = 1;
constexpr uint16_t block_id_notify = 2; // reserved. feature was removed. next net_version should be 3

constexpr uint16_t net_version = proto_explicit_sync;

enum connection_types : char {
    both,
    transactions_only,
    blocks_only
};

/**
*  For a while, network version was a 16 bit value equal to the second set of 16 bits
*  of the current build's git commit id. We are now replacing that with an integer protocol
*  identifier. Based on historical analysis of all git commit identifiers, the larges gap
*  between ajacent commit id values is shown below.
*  these numbers were found with the following commands on the master branch:
*
*  git log | grep "^commit" | awk '{print substr($2,5,4)}' | sort -u > sorted.txt
*  rm -f gap.txt; prev=0; for a in $(cat sorted.txt); do echo $prev $((0x$a - 0x$prev)) $a >> gap.txt; prev=$a; done; sort -k2 -n gap.txt | tail
*
*  DO NOT EDIT net_version_base OR net_version_range!
*/
constexpr uint16_t net_version_base = 0x04b5;
constexpr uint16_t net_version_range = 106;

/**
* Index by start_block_num
*/
struct peer_sync_state {
    explicit peer_sync_state(uint32_t start = 0, uint32_t end = 0, uint32_t last_acted = 0)
        :start_block( start ), end_block( end ), last( last_acted ),
        start_time(time_point::now())
    {}
    uint32_t     start_block;
    uint32_t     end_block;
    uint32_t     last; ///< last sent or received
    time_point   start_time; ///< time request made or received
};

// thread safe
class queued_buffer : boost::noncopyable {
public:
    void clear_write_queue() {
        std::lock_guard<std::mutex> g( _mtx );
        _write_queue.clear();
        _sync_write_queue.clear();
        _write_queue_size = 0;
    }

    void clear_out_queue() {
        std::lock_guard<std::mutex> g( _mtx );
        while ( _out_queue.size() > 0 ) {
        _out_queue.pop_front();
        }
    }

    uint32_t write_queue_size() const {
        std::lock_guard<std::mutex> g( _mtx );
        return _write_queue_size;
    }

    bool is_out_queue_empty() const {
        std::lock_guard<std::mutex> g( _mtx );
        return _out_queue.empty();
    }

    bool ready_to_send() const {
        std::lock_guard<std::mutex> g( _mtx );
        // if out_queue is not empty then async_write is in progress
        return ((!_sync_write_queue.empty() || !_write_queue.empty()) && _out_queue.empty());
    }

    // @param callback must not callback into queued_buffer
    bool add_write_queue( const std::shared_ptr<vector<char>>& buff,
                        std::function<void( boost::system::error_code, std::size_t )> callback,
                        bool to_sync_queue ) {
        std::lock_guard<std::mutex> g( _mtx );
        if( to_sync_queue ) {
        _sync_write_queue.push_back( {buff, callback} );
        } else {
        _write_queue.push_back( {buff, callback} );
        }
        _write_queue_size += buff->size();
        if( _write_queue_size > 2 * def_max_write_queue_size ) {
        return false;
        }
        return true;
    }

    void fill_out_buffer( std::vector<boost::asio::const_buffer>& bufs ) {
        std::lock_guard<std::mutex> g( _mtx );
        if( _sync_write_queue.size() > 0 ) { // always send msgs from sync_write_queue first
        fill_out_buffer( bufs, _sync_write_queue );
        } else { // postpone real_time write_queue if sync queue is not empty
        fill_out_buffer( bufs, _write_queue );
        EOS_ASSERT( _write_queue_size == 0, plugin_exception, "write queue size expected to be zero" );
        }
    }

    void out_callback( boost::system::error_code ec, std::size_t w ) {
        std::lock_guard<std::mutex> g( _mtx );
        for( auto& m : _out_queue ) {
        m.callback( ec, w );
        }
    }

private:
    struct queued_write;
    void fill_out_buffer( std::vector<boost::asio::const_buffer>& bufs,
                        deque<queued_write>& w_queue ) {
        while ( w_queue.size() > 0 ) {
        auto& m = w_queue.front();
        bufs.push_back( boost::asio::buffer( *m.buff ));
        _write_queue_size -= m.buff->size();
        _out_queue.emplace_back( m );
        w_queue.pop_front();
        }
    }

private:
    struct queued_write {
        std::shared_ptr<vector<char>> buff;
        std::function<void( boost::system::error_code, std::size_t )> callback;
    };

    mutable std::mutex  _mtx;
    uint32_t            _write_queue_size{0};
    deque<queued_write> _write_queue;
    deque<queued_write> _sync_write_queue; // sync_write_queue will be sent first
    deque<queued_write> _out_queue;

}; // queued_buffer

class endpoint_info_t {
public:
    string                      remote_endpoint_ip;
    string                      remote_endpoint_port;
    string                      local_endpoint_ip;
    string                      local_endpoint_port;
};

using message_buf_t = fc::message_buffer<1024*1024>;
class net_stream {
public:
    using init_callback_func = std::function<void(boost::system::error_code)>;
    using read_callback = void(boost::system::error_code, size_t);
    using read_callback_func = std::function<read_callback>;
    using write_callback = void(boost::system::error_code, size_t);
    using write_callback_func = std::function<write_callback>;

    virtual ~net_stream() {};

    virtual bool init(std::shared_ptr<strand_t> strand) = 0;

    virtual void close() = 0;

    virtual void read(message_buf_t &buffer, std::size_t min_size, read_callback_func cb) = 0;

    virtual void write(queued_buffer &buffer_queue, write_callback_func cb) = 0;

    /**
     * Check, if this stream is closed bor both writes and reads
     * @return true, if stream is closed entirely, false otherwise
     */
    // virtual bool is_closed() const = 0;

    /**
     * Close a stream, indicating we are not going to write to it anymore; the
     * other side, however, can write to it, if it was not closed from there
     * before
     * @param cb to be called, when the stream is closed, or error happens
     */
    // virtual void close(void_result_handler_func cb) = 0;

    // /**
    //  * @brief Close this stream entirely; this normally means an error happened,
    //  * so it should not be used just to close the stream
    //  */
    // virtual void reset() = 0;

    // /**
    //  * Set a new receive window size of this stream - how much unread bytes can
    //  * we have on our side of the stream
    //  * @param new_size for the window
    //  * @param cb to be called, when the operation succeeds of fails
    //  */
    // virtual void adjustWindowSize(uint32_t new_size,
    //                               VoidResultHandlerFunc cb) = 0;

    /**
     * Is that stream opened over a connection, which was an initiator?
     */
    // virtual outcome::result<bool> is_initiator() const = 0;

    // /**
    //  * Get a peer, which the stream is connected to
    //  * @return id of the peer
    //  */
    // virtual outcome::result<peer::PeerId> remotePeerId() const = 0;

    // /**
    //  * Get a local multiaddress
    //  * @return address or error
    //  */
    // virtual outcome::result<multi::Multiaddress> localMultiaddr() const = 0;

    // /**
    //  * Get a multiaddress, to which the stream is connected
    //  * @return multiaddress or error
    //  */
    // virtual outcome::result<multi::Multiaddress> remoteMultiaddr() const = 0;

    virtual bool is_init() const = 0;
    virtual const endpoint_info_t& get_endpoint_info() = 0;
  };


class connector_t {
public:
    virtual ~connector_t() {}
    using connection_callback =
        void(boost::system::error_code, std::shared_ptr<net_stream>);
    using handler_func = std::function<connection_callback>;

    virtual void connect(handler_func handler) = 0;
};

class tcp_connector: public connector_t, public std::enable_shared_from_this<tcp_connector> {
public:
    // TODO: ...
    void connect(connector_t::handler_func handler) override;

    static std::shared_ptr<tcp_connector> create(std::shared_ptr<strand_t> strand, const string &peer_addr);


    connection_types get_connection_type() const { return connection_type; };

private:
    std::shared_ptr<strand_t> strand_;
    string peer_addr;
    string host;
    string port;
    std::atomic<connection_types> connection_type{both};
    std::atomic<bool> connecting{false};
    std::shared_ptr<tcp::socket>              socket_;

    void set_connection_type( const string& peer_addr );
};

class tcp_stream: public net_stream, public std::enable_shared_from_this<tcp_stream> {
public:
    tcp_stream(std::shared_ptr<tcp::socket> socket, const string &peer_addr)
        : socket_(socket), peer_addr_(peer_addr) {}
    ~tcp_stream();

    bool init(std::shared_ptr<strand_t> strand) override;

    void close() override;
    /**
     * @brief Write exactly {@code} in.size() {@nocode} bytes.
     * Won't call \param cb before all are successfully written.
     * Returns immediately.
     * @param in data to write.
     * @param bytes number of bytes to write
     * @param cb callback with result of operation
     *
     * @note caller should maintain validity of an input buffer until callback
     * is executed. It is usually done with either wrapping buffer as shared
     * pointer, or having buffer as part of some class/struct, and using
     * enable_shared_from_this()
     */
    void write(queued_buffer &buffer_queue, write_callback_func cb) override;

    void read(message_buf_t &buffer, std::size_t min_size, read_callback_func cb) override;

    bool is_init() const override;

    const endpoint_info_t& get_endpoint_info() override;
private:
    std::shared_ptr<strand_t> strand_;
    std::shared_ptr<tcp::socket> socket_; // only accessed through strand after construction
    std::string peer_addr_;
    endpoint_info_t endpoint_info_;
    bool is_init_;

    void update_endpoints();
};

class tcp_listener: public std::enable_shared_from_this<tcp_listener> {
public:
    using connection_callback =
        void(boost::system::error_code, std::shared_ptr<net_stream>, const std::string&);
    using handler_func = std::function<connection_callback>;

    bool init(std::shared_ptr<strand_t> strand);

    void accept(handler_func handler);

    void close();
private:
    std::shared_ptr<strand_t> strand_;
    std::unique_ptr<tcp::acceptor>        acceptor_;
};

class connection : public std::enable_shared_from_this<connection> {
public:
    explicit connection( string endpoint );
    explicit connection(std::shared_ptr<net_stream> stream);
    connection();

    ~connection() {}

    bool start_session();

    const string& peer_address() const { return peer_addr; } // thread safe, const

    void set_connection_type( const string& peer_addr );
    bool is_transactions_only_connection()const { return connection_type == transactions_only; }
    bool is_blocks_only_connection()const { return connection_type == blocks_only; }

private:
    static const string unknown;

    std::optional<peer_sync_state>    peer_requested;  // this peer is requesting info from us

    std::atomic<bool>                         connected_{false};

    const string            peer_addr;

    std::atomic<connection_types>             connection_type{both};
    endpoint_info_t             endpoint_info_;

public:
    std::shared_ptr<strand_t> strand;

    fc::message_buffer<1024*1024>    pending_message_buffer;
    std::atomic<std::size_t>         outstanding_read_bytes{0}; // accessed only from strand threads

    queued_buffer           buffer_queue;

    std::atomic<uint32_t>   trx_in_progress_size{0};
    const uint32_t          connection_id;
    int16_t                 sent_handshake_count = 0;
    std::atomic<bool>       connecting{false};
    std::atomic<bool>       syncing{false};
    uint16_t                protocol_version = 0;
    uint16_t                consecutive_rejected_blocks = 0;
    std::atomic<uint16_t>   consecutive_immediate_connection_close = 0;

    std::mutex                            response_expected_timer_mtx;
    boost::asio::steady_timer             response_expected_timer;

    std::atomic<go_away_reason>           no_retry{no_reason};

    mutable std::mutex          conn_mtx; //< mtx for last_req .. local_endpoint_port
    optional<request_message>   last_req;
    handshake_message           last_handshake_recv;
    handshake_message           last_handshake_sent;
    block_id_type               fork_head;
    uint32_t                    fork_head_num{0};
    fc::time_point              last_close;
    fc::sha256                  conn_node_id;

    connection_status get_status()const;

    const string& get_remote_endpoint_ip() const {    return endpoint_info_.remote_endpoint_ip; }

    /** \name Peer Timestamps
     *  Time message handling
     *  @{
     */
    // Members set from network data
    tstamp                         org{0};          //!< originate timestamp
    tstamp                         rec{0};          //!< receive timestamp
    tstamp                         dst{0};          //!< destination timestamp
    tstamp                         xmt{0};          //!< transmit timestamp
    /** @} */

    bool connected();
    bool current();

    /// @param reconnect true if we should try and reconnect immediately after close
    /// @param shutdown true only if plugin is shutting down
    void close( bool reconnect = true, bool shutdown = false );
private:
    static void _close( connection* self, bool reconnect, bool shutdown ); // for easy capture
public:

    bool populate_handshake( handshake_message& hello, bool force );

    bool resolve_and_connect();
    // void connect( const std::shared_ptr<tcp::resolver>& resolver, tcp::resolver::results_type endpoints );
    void start_read_message();

    /** \brief Process the next message from the pending message buffer
     *
     * Process the next message from the pending_message_buffer.
     * message_length is the already determined length of the data
     * part of the message that will handle the message.
     * Returns true is successful. Returns false if an error was
     * encountered unpacking or processing the message.
     */
    bool process_next_message(uint32_t message_length);

    void send_handshake( bool force = false );

    /** \name Peer Timestamps
     *  Time message handling
     */
    /**  \brief Populate and queue time_message
     */
    void send_time();
    /** \brief Populate and queue time_message immediately using incoming time_message
     */
    void send_time(const time_message& msg);
    /** \brief Read system time and convert to a 64 bit integer.
     *
     * There are only two calls on this routine in the program.  One
     * when a packet arrives from the network and the other when a
     * packet is placed on the send queue.  Calls the kernel time of
     * day routine and converts to a (at least) 64 bit integer.
     */
    static tstamp get_time() {
        return std::chrono::system_clock::now().time_since_epoch().count();
    }
    /** @} */

    const string peer_name();

    void blk_send_branch( const block_id_type& msg_head_id );
    void blk_send_branch_impl( uint32_t msg_head_num, uint32_t lib_num, uint32_t head_num );
    void blk_send(const block_id_type& blkid);
    void stop_send();

    void enqueue( const net_message &msg );
    void enqueue_block( const signed_block_ptr& sb, bool to_sync_queue = false);
    void enqueue_buffer( const std::shared_ptr<std::vector<char>>& send_buffer,
                        go_away_reason close_after_send,
                        bool to_sync_queue = false);
    void cancel_sync(go_away_reason);
    void flush_queues();
    bool enqueue_sync_block();
    void request_sync_blocks(uint32_t start, uint32_t end);

    void cancel_wait();
    void sync_wait();
    void fetch_wait();
    void sync_timeout(boost::system::error_code ec);
    void fetch_timeout(boost::system::error_code ec);

    void queue_write(const std::shared_ptr<vector<char>>& buff,
                    std::function<void(boost::system::error_code, std::size_t)> callback,
                    bool to_sync_queue = false);
    void do_queue_write();

    static bool is_valid( const handshake_message& msg );

    void handle_message( const handshake_message& msg );
    void handle_message( const chain_size_message& msg );
    void handle_message( const go_away_message& msg );
    /** \name Peer Timestamps
     *  Time message handling
     *  @{
     */
    /** \brief Process time_message
     *
     * Calculate offset, delay and dispersion.  Note carefully the
     * implied processing.  The first-order difference is done
     * directly in 64-bit arithmetic, then the result is converted
     * to floating double.  All further processing is in
     * floating-double arithmetic with rounding done by the hardware.
     * This is necessary in order to avoid overflow and preserve precision.
     */
    void handle_message( const time_message& msg );
    /** @} */
    void handle_message( const notice_message& msg );
    void handle_message( const request_message& msg );
    void handle_message( const sync_request_message& msg );
    void handle_message( const signed_block& msg ) = delete; // signed_block_ptr overload used instead
    void handle_message( const block_id_type& id, signed_block_ptr msg );
    void handle_message( const packed_transaction& msg ) = delete; // packed_transaction_ptr overload used instead
    void handle_message( packed_transaction_ptr msg );

    void process_signed_block( const block_id_type& id, signed_block_ptr msg );

    fc::variant_object get_logger_variant()  {
        fc::mutable_variant_object mvo;
        mvo( "_name", peer_name());
        std::lock_guard<std::mutex> g_conn( conn_mtx );
        mvo( "_id", conn_node_id )
        ( "_sid", conn_node_id.str().substr( 0, 7 ) )
        ( "_ip", endpoint_info_.remote_endpoint_ip )
        ( "_port", endpoint_info_.remote_endpoint_port )
        ( "_lip", endpoint_info_.local_endpoint_ip )
        ( "_lport", endpoint_info_.local_endpoint_port );
        return mvo;
    }
private:
    std::shared_ptr<connector_t> connector_;
    std::shared_ptr<net_stream> stream_;
};

template< typename T>
inline std::shared_ptr<std::vector<char>> create_send_buffer( uint32_t which, const T& v ) {
    // match net_message static_variant pack
    const uint32_t which_size = fc::raw::pack_size( unsigned_int( which ) );
    const uint32_t payload_size = which_size + fc::raw::pack_size( v );

    const char* const header = reinterpret_cast<const char* const>(&payload_size); // avoid variable size encoding of uint32_t
    constexpr size_t header_size = sizeof( payload_size );
    static_assert( header_size == message_header_size, "invalid message_header_size" );
    const size_t buffer_size = header_size + payload_size;

    auto send_buffer = std::make_shared<vector<char>>( buffer_size );
    fc::datastream<char*> ds( send_buffer->data(), buffer_size );
    ds.write( header, header_size );
    fc::raw::pack( ds, unsigned_int( which ) );
    fc::raw::pack( ds, v );

    return send_buffer;
}

std::shared_ptr<std::vector<char>> create_send_buffer( const signed_block_ptr& sb );

std::shared_ptr<std::vector<char>> create_send_buffer( const packed_transaction& trx );

}// namespace eosio
