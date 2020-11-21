// Copyright (c) 2009-2010 Satoshi Nakamoto
// Copyright (c) 2020-2021 The nchain Developers
// Distributed under the MIT/X11 software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.
// #include <eosio/chain/types.hpp>

#ifndef NET_PLUGIN_NET_TRANSPORT_HPP
#define NET_PLUGIN_NET_TRANSPORT_HPP

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

namespace eosio {

using namespace std;
using strand_t = boost::asio::io_context::strand;
using boost::asio::ip::tcp;
using boost::asio::ip::address_v4;
using boost::asio::ip::host_name;
using boost::multi_index_container;

class queued_buffer;

enum connection_types : char {
    both,
    transactions_only,
    blocks_only
};

class endpoint_info_t {
public:
    string                      remote_endpoint_ip;
    string                      remote_endpoint_port;
    string                      local_endpoint_ip;
    string                      local_endpoint_port;
};

using message_buf_t = fc::message_buffer<1024*1024>;

class net_transport {
public:
    using init_callback_func = std::function<void(boost::system::error_code)>;
    using read_callback = void(boost::system::error_code, size_t);
    using read_callback_func = std::function<read_callback>;
    using write_callback = void(boost::system::error_code, size_t);
    using write_callback_func = std::function<write_callback>;

    virtual ~net_transport() {};

    virtual bool init(std::shared_ptr<strand_t> strand) = 0;

    virtual void close() = 0;

    virtual void read(message_buf_t &buffer, std::size_t min_size, read_callback_func cb) = 0;

    virtual void write(queued_buffer &buffer_queue, write_callback_func cb) = 0;

    /**
     * Check, if this transport is closed bor both writes and reads
     * @return true, if transport is closed entirely, false otherwise
     */
    // virtual bool is_closed() const = 0;

    /**
     * Close a transport, indicating we are not going to write to it anymore; the
     * other side, however, can write to it, if it was not closed from there
     * before
     * @param cb to be called, when the transport is closed, or error happens
     */
    // virtual void close(void_result_handler_func cb) = 0;

    // /**
    //  * @brief Close this transport entirely; this normally means an error happened,
    //  * so it should not be used just to close the transport
    //  */
    // virtual void reset() = 0;

    // /**
    //  * Set a new receive window size of this transport - how much unread bytes can
    //  * we have on our side of the transport
    //  * @param new_size for the window
    //  * @param cb to be called, when the operation succeeds of fails
    //  */
    // virtual void adjustWindowSize(uint32_t new_size,
    //                               VoidResultHandlerFunc cb) = 0;

    /**
     * Is that transport opened over a connection, which was an initiator?
     */
    // virtual outcome::result<bool> is_initiator() const = 0;

    // /**
    //  * Get a peer, which the transport is connected to
    //  * @return id of the peer
    //  */
    // virtual outcome::result<peer::PeerId> remotePeerId() const = 0;

    // /**
    //  * Get a local multiaddress
    //  * @return address or error
    //  */
    // virtual outcome::result<multi::Multiaddress> localMultiaddr() const = 0;

    // /**
    //  * Get a multiaddress, to which the transport is connected
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
        void(boost::system::error_code, std::shared_ptr<net_transport>);
    using handler_func = std::function<connection_callback>;

    virtual void connect(handler_func handler) = 0;

    virtual const std::string& peer_address() const = 0;
};

class tcp_connector: public connector_t, public std::enable_shared_from_this<tcp_connector> {
public:
    // TODO: ...
    void connect(connector_t::handler_func handler) override;

    static std::shared_ptr<tcp_connector> create(std::shared_ptr<strand_t> strand, const string &peer_addr);

    const std::string& peer_address() const override;

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

class tcp_transport: public net_transport, public std::enable_shared_from_this<tcp_transport> {
public:
    tcp_transport(std::shared_ptr<tcp::socket> socket, const string &peer_addr)
        : socket_(socket), peer_addr_(peer_addr) {}
    ~tcp_transport();

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
        void(boost::system::error_code, std::shared_ptr<net_transport>, const std::string&);
    using handler_func = std::function<connection_callback>;

    bool init(std::shared_ptr<strand_t> strand);

    void accept(handler_func handler);

    void close();
private:
    std::shared_ptr<strand_t> strand_;
    std::unique_ptr<tcp::acceptor>        acceptor_;
};

} // namespace eosio

#endif//NET_PLUGIN_NET_TRANSPORT_HPP