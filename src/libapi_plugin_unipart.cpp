// =-=-=-=-=-=-=-
// irods includes
#include "apiHandler.hpp"
#include "rodsPackInstruct.h"
#include "objStat.h"
#include "physPath.hpp"

#include "irods_stacktrace.hpp"
#include "irods_server_api_call.hpp"
#include "irods_re_serialization.hpp"
#include "irods_api_envelope.hpp"
#include "irods_api_endpoint.hpp"
#include "irods_virtual_path.hpp"
#include "irods_resource_manager.hpp"
#include "irods_file_object.hpp"

//#include "irods_hostname.hpp"
bool hostname_resolves_to_local_address(const char* hostname);
extern irods::resource_manager resc_mgr;

#include "irods_multipart_response.hpp"
#include "irods_unipart_request.hpp"

#include "boost/lexical_cast.hpp"
#include "boost/filesystem.hpp"
#include "boost/filesystem/operations.hpp"
#include "boost/filesystem/path.hpp"

// =-=-=-=-=-=-=-
// stl includes
#include <sstream>
#include <string>
#include <iostream>
#include <future>
#include <thread>

static const irods::message_broker::data_type ACK_MSG = {'A', 'C', 'K'};
static const irods::message_broker::data_type QUIT_MSG = {'q', 'u', 'i', 't'};
static const irods::message_broker::data_type FINALIZE_MSG = {'F', 'I', 'N', 'A', 'L', 'I', 'Z', 'E'};
static const irods::message_broker::data_type ERROR_MSG = {'e', 'r', 'r', 'o', 'r'};

void foo_bar() {

}

void transfer_executor_client(
    const int                               _file_desc,
    const int                               _port,
    const std::string&                      _host_name,
    const std::vector<irods::part_request>& _part_queue) {
    typedef irods::message_broker::data_type data_t;
    
    try {
        irods::message_broker bro("ZMQ_REQ");
        std::stringstream conn_sstr;
        conn_sstr << "tcp://" << _host_name << ":";
        conn_sstr << _port;
        bro.connect(conn_sstr.str());

        size_t read_size = 4*1024*1024; // TODO: parameterize
        for(auto& part : _part_queue) {
            data_t snd_msg(read_size);
            // build a unipart request    
            irods::unipart_request uni_req;
            uni_req.transfer_complete = false;
            uni_req.file_size = part.file_size;
            uni_req.restart_offset = part.restart_offset;
            uni_req.original_offset = part.original_offset;
            uni_req.resource_hierarchy = part.resource_hierarchy;
            uni_req.destination_logical_path = part.destination_logical_path;

            // offset into the source file for this thread
            off_t offset_value = uni_req.restart_offset+uni_req.original_offset;
            int status = lseek(_file_desc, offset_value, SEEK_SET);
            if(status < 0) {
                std::cerr << __FUNCTION__ << ":" << __LINE__ << " - failed to seek to: " << offset_value << ", errno: " << errno << std::endl;
                continue;
            }

            // encode the unipart request
            auto out = avro::memoryOutputStream();
            auto enc = avro::binaryEncoder();
            enc->init( *out );
            avro::encode( *enc, uni_req );
            auto data = avro::snapshot( *out );

            // send request and await response 
            bro.send(*data);

            data_t rcv_msg;
            bro.receive(rcv_msg);

            ssize_t bytes_read = 0;
            while(bytes_read < part.file_size) {
                ssize_t sz = read(_file_desc, snd_msg.data(), read_size);
                if(sz < 0) {
                    std::cerr << "Failed to read with errno: " << errno << std::endl;
                    break;
                }

                bytes_read += sz;

                bro.send(snd_msg);
                
                data_t rcv_msg;
                bro.receive(rcv_msg);
                if(ACK_MSG != rcv_msg) {
                    std::cerr << "Received error from server" << std::endl;
                    break;
                }

            } // while

        } // for

        // notify the server side that we are done transmitting parts
        irods::unipart_request uni_req;
        uni_req.transfer_complete = true;

        // encode the unipart request
        auto out = avro::memoryOutputStream();
        auto enc = avro::binaryEncoder();
        enc->init( *out );
        avro::encode( *enc, uni_req );
        auto data = avro::snapshot( *out );

        // send request and await response 
        bro.send(*data);
        data_t rcv_msg;
        bro.receive(rcv_msg);

    }
    catch(const zmq::error_t& _e) {
        THROW(SYS_SOCK_CONNECT_ERR, _e.what());
    }
    catch(const boost::bad_any_cast& _e) {
        std::cerr << _e.what() << std::endl;
        THROW(INVALID_ANY_CAST, _e.what());
        throw;
    }
    catch(const irods::exception& _e) {
        std::cerr << _e.what() << std::endl;
        throw;
    }
} // transfer_executor_client

void unipart_executor_client(
        irods::api_endpoint*  _endpoint ) {
    typedef irods::message_broker::data_type data_t;
    try {
        // open the control channel back to the multipart client executor
        irods::message_broker cmd_skt("ZMQ_REP", _endpoint->ctrl_ctx());
        cmd_skt.connect("inproc://client_comms");

        // extract the payload
        irods::multipart_response mp_resp;
        _endpoint->payload<irods::multipart_response>(mp_resp);
        
        // guarantee that all parts have same host_name and resource_hierarchy
        // as all parts route to the one resource for reassembly in this plugin
        for(auto& p : mp_resp.parts) {
            p.host_name = mp_resp.parts.begin()->host_name;
            p.resource_hierarchy = mp_resp.parts.begin()->resource_hierarchy;
        }

        // deal parts out to thread task queues
        std::vector<std::vector<irods::part_request>> part_queues(mp_resp.number_of_threads); 
        std::vector<std::vector<irods::part_request>>::iterator part_itr = part_queues.begin();
        for(size_t idx = 0; idx < mp_resp.parts.size(); ++idx) {
            part_itr->push_back(mp_resp.parts[idx]);
            part_itr++;
            if(part_queues.end() == part_itr) {
                part_itr = part_queues.begin();
            }
        }

        // open the file for read
        int file_desc = open(mp_resp.source_physical_path.c_str(), O_RDONLY);
        if(-1 == file_desc) {
            std::string msg("failed to open file [");
            msg += mp_resp.source_physical_path;
            msg += "]";
            THROW(FILE_OPEN_ERR, msg);
        }

        // start threads to transmit part data
        std::vector<std::unique_ptr<std::thread>> threads;
        for(int tid = 0; tid <mp_resp.number_of_threads; ++tid) {
            threads.emplace_back(std::make_unique<std::thread>(
                transfer_executor_client,
                file_desc,
                mp_resp.port_list[tid],
                mp_resp.parts.begin()->host_name,
                part_queues[tid])); 
        }

        // wait for all threads to complete
        for( size_t tid = 0; tid < threads.size(); ++tid) {
            threads[tid]->join();
        }

        // close the source file
        close(file_desc);

        // wait for message request
        data_t rcv_msg;
        cmd_skt.receive(rcv_msg);

        // notify that the transfer is complete
        cmd_skt.send(FINALIZE_MSG);
    }
    catch(const zmq::error_t& _e) {
        //TODO: notify client of failure
        irods::log(LOG_ERROR, _e.what());
        _endpoint->done(true);
        THROW(SYS_SOCK_CONNECT_ERR, _e.what());
    }
    catch(const boost::bad_any_cast& _e) {
        //TODO: notify client of failure
        irods::log(LOG_ERROR, _e.what());
        _endpoint->done(true);
        THROW(INVALID_ANY_CAST, _e.what());
    }

    _endpoint->done(true);
 
} // unipart_executor_client

#ifdef RODS_SERVER
std::string create_physical_path(
    irods::resource_ptr _resc,
    const std::string&  _logical_path) {

    std::string vault_path;
    irods::error ret = _resc->get_property<std::string>(
                           irods::RESOURCE_PATH,
                           vault_path);
    if(!ret.ok()) {
        THROW(ret.code(), ret.result());
    }

    char out_path[MAX_NAME_LEN];
    int status = setPathForGraftPathScheme(
                     const_cast<char*>(_logical_path.c_str()),
                     vault_path.c_str(),
                     0, 0, 1, out_path);
    if(status < 0) {
        THROW(status, "setPathForGraftPathScheme failed");
    }

    return out_path;
} // create_physical_path
#endif

void transfer_executor_server(
    rsComm_t*                               _comm,
    std::promise<int>*                      _port_promise) {
#ifdef RODS_SERVER
    typedef irods::message_broker::data_type data_t;
    
    try {
        irods::message_broker bro("ZMQ_REP");

        // get the port range from server configuration
        const int start_port = irods::get_server_property<const int>(
                                   irods::CFG_SERVER_PORT_RANGE_START_KW);
        const int  end_port = irods::get_server_property<const int>(
                                  irods::CFG_SERVER_PORT_RANGE_END_KW);

        // bind to a port in the given range to handle the message passing
        int port = bro.bind_to_port_in_range(start_port, end_port);
        _port_promise->set_value(port);

        while( true ) { // TODO: add timeout?
            // wait for next part request
            data_t rcv_msg;
            bro.receive(rcv_msg);

            auto in = avro::memoryInputStream(
                          &rcv_msg[0],
                          rcv_msg.size());
            auto dec = avro::binaryDecoder();
            dec->init( *in );
            irods::unipart_request uni_req;
            avro::decode( *dec, uni_req );

            if(uni_req.transfer_complete) {
                bro.send(ACK_MSG);
                break;
            }

            // determine leaf resource
            irods::hierarchy_parser hp;
            hp.set_string(uni_req.resource_hierarchy);
            std::string leaf_name;
            hp.last_resc(leaf_name);

            // resolve the leaf resource in question
            irods::resource_ptr resc;
            irods::error ret = resc_mgr.resolve(leaf_name, resc);
            if(!ret.ok()) {
                THROW(ret.code(), ret.result());
            }

            // determine physical path
            std::string phy_path = create_physical_path(resc, uni_req.destination_logical_path);

            // create the file object fco
            irods::file_object_ptr file_obj( new irods::file_object(
                                       _comm,
                                       "temp_object_path",
                                       phy_path,
                                       uni_req.resource_hierarchy,
                                       0, getDefFileMode(), O_WRONLY | O_CREAT));

            ret = resc->call(_comm, irods::RESOURCE_OP_OPEN, file_obj);
            if(!ret.ok()) {
                THROW(ret.code(), ret.result());
            }

            // wire file descriptor into the FCO
            file_obj->file_descriptor(ret.code());

            // acknowlege part request and begin sending data
            bro.send(ACK_MSG);

            // start writing things down
            bool done_flag = false;
            int64_t bytes_written = 0;
            while(!done_flag) {
                data_t rcv_data;
                bro.receive(rcv_data);

                ret = resc->call<void*,int>(
                          _comm,
                          irods::RESOURCE_OP_WRITE,
                          file_obj,
                          rcv_data.data(),
                          rcv_data.size());
                if(!ret.ok()) {
                    bro.send(ERROR_MSG);
                    THROW(ret.code(), ret.result());
                }

                bro.send(ACK_MSG);

                bytes_written += rcv_data.size();
                if(bytes_written >= uni_req.file_size) {
                    done_flag = true;
                }
            }// while

            // close object
            resc->call(_comm, irods::RESOURCE_OP_CLOSE, file_obj);
            if(!ret.ok()) {
                THROW(ret.code(), ret.result());
            }

            // TODO: register physical path and data size

        } // while

    }
    catch(const irods::exception& _e) {
        throw;
    }
#endif
} // transfer_executor_server

void unipart_executor_server(
        irods::api_endpoint*  _endpoint ) {
    typedef irods::message_broker::data_type data_t;
    try {
        rsComm_t* comm = nullptr;
        _endpoint->comm<rsComm_t*>(comm);

        // open the control channel back to the multipart server executor
        irods::message_broker cmd_skt("ZMQ_REP", _endpoint->ctrl_ctx());
        cmd_skt.connect("inproc://server_comms");

        // extract the payload
        irods::multipart_response mp_resp;
        _endpoint->payload<irods::multipart_response>(mp_resp);
        if(mp_resp.parts.empty()) {
            THROW(SYS_INVALID_INPUT_PARAM, "empty parts array");
        }

        if(mp_resp.number_of_threads <= 0) {
            THROW(SYS_INVALID_INPUT_PARAM, "invalid number of threads");
        }

        // guarantee that all parts have same host_name and resource_hierarchy
        // for this plugin all parts route as single resource for reassembly
        for(auto& p : mp_resp.parts) {
            p.host_name = mp_resp.parts.begin()->host_name;
            p.resource_hierarchy = mp_resp.parts.begin()->resource_hierarchy;
        }

        // by convention choose the first resource as our target
        const std::string& host_name = mp_resp.parts.begin()->host_name;
        if(hostname_resolves_to_local_address(host_name.c_str())) {
            std::vector<std::unique_ptr<std::thread>> threads(mp_resp.number_of_threads); 
            std::vector<std::promise<int>>            promises(mp_resp.number_of_threads);

            // start threads to recieve part data
            for(int tid = 0; tid <mp_resp.number_of_threads; ++tid) {
                threads[tid] = std::make_unique<std::thread>(
                                   transfer_executor_server,
                                   comm,
                                   &promises[tid]);
            }
            
            // wait until all ports are bound and fill in
            // the ports in the response object
            for( size_t pid = 0; pid < promises.size(); ++pid) {
                auto f = promises[pid].get_future();
                f.wait();
                mp_resp.port_list.push_back( f.get() );
            }

            data_t rcv_msg;
            cmd_skt.receive(rcv_msg);

            // respond with the object
            auto out = avro::memoryOutputStream();
            auto enc = avro::binaryEncoder();
            enc->init( *out );
            avro::encode( *enc, mp_resp );
            auto repl_data = avro::snapshot( *out );

            cmd_skt.send(*repl_data);

            // wait for all threads to complete
            for( size_t tid = 0; tid < threads.size(); ++tid) {
                threads[tid]->join();
            }

            // if nothing has exploded, reassemble the parts
    
        }
        else {
            // redirect, we are not the correct server
            std::cout << "XXXX - " << __FUNCTION__ << ":" << __LINE__ << "REDIRECT for host_name: " << host_name << std::endl;
        }
    }
    catch(const zmq::error_t& _e) {
// TODO: stat and update part objects if failed
//TODO: notify client of failure
        irods::log(LOG_ERROR, _e.what());
        _endpoint->done(true);
        THROW(SYS_SOCK_CONNECT_ERR, _e.what());
    }
    catch(const boost::bad_any_cast& _e) {
// TODO: stat and update part objects if failed
//TODO: notify client of failure
        irods::log(LOG_ERROR, _e.what());
        _endpoint->done(true);
        THROW(INVALID_ANY_CAST, _e.what());
    }

    _endpoint->done(true);
} // unipart_executor_server

void unipart_executor_server_to_server(
        irods::api_endpoint*  _endpoint ) {
    _endpoint->done(true);
    return;
} // unipart_executor_server_to_server


class unipart_api_endpoint : public irods::api_endpoint {
    public:
        // =-=-=-=-=-=-=-
        // provide thread executors to the invoke() method
        void capture_executors(
                thread_executor& _cli,
                thread_executor& _svr,
                thread_executor& _svr_to_svr) {
            _cli        = unipart_executor_client;
            _svr        = unipart_executor_server;
            _svr_to_svr = unipart_executor_server_to_server;
        }

        unipart_api_endpoint(const std::string& _ctx) :
            irods::api_endpoint(_ctx) {
        }

        ~unipart_api_endpoint() {
        }

        // =-=-=-=-=-=-=-
        // used for client-side initialization
        void init_and_serialize_payload(
            const std::vector<std::string>& _args,
            std::vector<uint8_t>&           _out) {
        }

        // =-=-=-=-=-=-=-
        // used for server-side initialization
        void decode_and_assign_payload(
            const std::vector<uint8_t>& _in) {
        }

        // =-=-=-=-=-=-=-
        // provide an error code and string to the client
        int status(rError_t* _err) {
            if(status_ < 0) {
                addRErrorMsg(
                    _err,
                    status_,
                    error_message_.str().c_str());
            }
            return status_;
        }

    private:
        int status_;
        std::stringstream error_message_;

}; // class api_endpoint

extern "C" {
    irods::api_endpoint* plugin_factory(
        const std::string&,//_inst_name
        const std::string& _context ) {
            return new unipart_api_endpoint(_context);
    }
};

