// =-=-=-=-=-=-=-
// irods includes
#include "apiHandler.hpp"
#include "rodsPackInstruct.h"
#include "objStat.h"
#include "physPath.hpp"
#include "rsModDataObjMeta.hpp"
#include "rsFileStat.hpp"
#include "rsRegDataObj.hpp"
#include "rcMisc.h"
#include "rsDataObjUnlink.hpp"
#include "rsRmColl.hpp"

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
static const irods::message_broker::data_type PROG_MSG = {'p', 'r', 'o', 'g', 'r', 'e', 's', 's'};

void transfer_executor_client(
    const std::string&                      _file_name,
    const int                               _port,
    const std::string&                      _host_name,
    const std::string                       _cmd_conn_str,
    zmq::context_t*                         _zmq_ctx,
    const std::vector<irods::part_request>& _part_queue) {
    
    typedef irods::message_broker::data_type data_t;
    
    try {
        irods::message_broker cmd_skt("ZMQ_REP", _zmq_ctx);
        cmd_skt.connect(_cmd_conn_str);

        // open the file for read
        int file_desc = open(_file_name.c_str(), O_RDONLY);
        if(-1 == file_desc) {
            std::string msg("failed to open file [");
            msg += _file_name;
            msg += "]";
            THROW(FILE_OPEN_ERR, msg);
        }
   
        irods::message_broker bro("ZMQ_REQ");
        std::stringstream conn_sstr;
        conn_sstr << "tcp://" << _host_name << ":";
        conn_sstr << _port;
        bro.connect(conn_sstr.str());

        //bool quit_received_flag      = false;
        const ssize_t block_size = 4*1024*1024; // TODO: parameterize
        for(auto& part : _part_queue) {
            /*data_t cmd_rcv_msg;
            cmd_skt.receive(cmd_rcv_msg, ZMQ_DONTWAIT);
            if(cmd_rcv_msg.size() > 0) {
                if(QUIT_MSG == cmd_rcv_msg) {
                    cmd_skt.send(ACK_MSG);
                    quit_received_flag = true;
                    break;
                }
            }*/

            // build a unipart request    
            irods::unipart_request uni_req;
            uni_req.transfer_complete = false;
            uni_req.file_size = part.file_size;
            uni_req.restart_offset = part.restart_offset;
            uni_req.original_offset = part.original_offset;
            uni_req.resource_hierarchy = part.resource_hierarchy;
            uni_req.destination_logical_path = part.destination_logical_path;
            uni_req.destination_physical_path = part.destination_physical_path;

            // offset into the source file for this thread
            off_t offset_value = uni_req.restart_offset+uni_req.original_offset;
            int status = lseek(file_desc, offset_value, SEEK_SET);
            if(status < 0) {
                std::cerr << __FUNCTION__
                          << ":" << __LINE__
                          << " - failed to seek to: "
                          << offset_value << ", errno: "
                          << errno << std::endl;
                continue;
            }

            // encode the unipart request
            auto out = avro::memoryOutputStream();
            auto enc = avro::binaryEncoder();
            enc->init( *out );
            avro::encode( *enc, uni_req );
            auto uni_req_data = avro::snapshot( *out );

            // send request and await response 
            bro.send(*uni_req_data);

            data_t ack_rcv_msg;
            bro.receive(ack_rcv_msg);
            if(ACK_MSG != ack_rcv_msg) {
                // TODO: process error here
            }

            ssize_t read_size     = block_size;
            ssize_t bytes_to_read = part.file_size;
            while(bytes_to_read > 0) {
                // handle possible incoming command requests
                /*data_t cmd_rcv_msg;
                cmd_skt.receive(cmd_rcv_msg, ZMQ_DONTWAIT);
                if(cmd_rcv_msg.size() > 0) {
                    if(QUIT_MSG == cmd_rcv_msg) {
                        cmd_skt.send(ACK_MSG);
                        quit_received_flag = true;
                        break;

                    }
                    else if(PROG_MSG == cmd_rcv_msg) {
                        // TODO: handle progress request
                    }
                }*/

                // read the data - block size or remainder
                data_t snd_msg(read_size);
                ssize_t sz = read(file_desc, snd_msg.data(), read_size);
                if(sz < 0) {
                    std::cerr << "Failed to read with errno: " << errno << std::endl;
                    break;
                }

                // ship the data to the server side
                bro.send(snd_msg);
                
                // receive acknowledgement of data
                data_t ack_rcv_msg;
                bro.receive(ack_rcv_msg);
                if(ACK_MSG != ack_rcv_msg) {
                    std::cerr << "Received error from server: " 
                              << ack_rcv_msg << std::endl;
                    break;
                }

                // keep track of blocks read and the remainder
                bytes_to_read -= sz;
                if(bytes_to_read < block_size) {
                    read_size = bytes_to_read;
                }
            } // while

            // if we got a quit while transporting a part, quit gracefully
            /*if(quit_received_flag) {
                break;
            }*/

        } // for

        // close the source file
        close(file_desc);

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
        std::cerr << _e.what() << std::endl;
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
    //typedef irods::message_broker::data_type data_t;
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

        // TODO: deal parts out in a serial fashion to cluster sequential reads in one thread
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

        zmq::context_t xport_zmq_ctx(mp_resp.parts.size()+1);

        // TODO: create vector of message brokers from local context
        std::vector<std::unique_ptr<irods::message_broker>> xport_skts;

        std::vector<std::unique_ptr<std::thread>> threads;
        for(int tid = 0; tid <mp_resp.number_of_threads; ++tid) {
            xport_skts.push_back(std::make_unique<irods::message_broker>("ZMQ_REQ", &xport_zmq_ctx));
            std::stringstream conn_sstr;
            conn_sstr << "inproc://xport_client_to_executors_";
            conn_sstr << tid;
            (*xport_skts.rbegin())->bind(conn_sstr.str());

            // TODO: pass local context to thread for ipc broker
            threads.emplace_back(std::make_unique<std::thread>(
                transfer_executor_client,
                mp_resp.source_physical_path,
                mp_resp.port_list[tid],
                mp_resp.parts.begin()->host_name,
                conn_sstr.str(),
                &xport_zmq_ctx,
                part_queues[tid])); 
        } // for thread id

        /*bool quit_received_flag = false;
        // receive and process commands
        while(true) {
            data_t cli_msg;
            cmd_skt.receive(cli_msg, ZMQ_DONTWAIT);

            data_t rcv_msg; 
            // forward message from multipart client to transport threads
            for(auto& skt : xport_skts) { 
                skt->send(cli_msg);
                skt->receive(rcv_msg);
                // TODO: process results
            }

            if(QUIT_MSG == cli_msg) {
                cmd_skt.send(ACK_MSG);
                quit_received_flag = true;
                break;
            }
            else {
            }

            cmd_skt.send(ACK_MSG);
        }*/ // while

        // wait for all threads to complete
        for( size_t tid = 0; tid < threads.size(); ++tid) {
            threads[tid]->join();
        }

    }
    catch(const zmq::error_t& _e) {
        //TODO: notify client of failure
        std::cerr << _e.what() << std::endl;
        _endpoint->done(true);
        THROW(SYS_SOCK_CONNECT_ERR, _e.what());
    }
    catch(const boost::bad_any_cast& _e) {
        //TODO: notify client of failure
        std::cerr << _e.what() << std::endl;
        _endpoint->done(true);
        THROW(INVALID_ANY_CAST, _e.what());
    }
    catch(const irods::exception& _e) {
        //TODO: notify client of failure
        std::cerr << _e.what() << std::endl;
        _endpoint->done(true);
        throw;
    }


    _endpoint->done(true);
 


} // unipart_executor_client

#ifdef RODS_SERVER
std::string make_physical_path(
    const std::string& _resource_hierarchy,
    const std::string& _destination_logical_path) {
    // determine leaf resource
    irods::hierarchy_parser hp;
    hp.set_string(_resource_hierarchy);
    std::string leaf_name;
    hp.last_resc(leaf_name);

    // resolve resource for leaf
    irods::resource_ptr leaf_resc;
    irods::error ret = resc_mgr.resolve(
                           leaf_name,
                           leaf_resc);
    if(!ret.ok()) {
        THROW(ret.code(), ret.result());
    }

    // extract the vault path from the resource
    std::string vault_path;
    leaf_resc->get_property<std::string>(
        irods::RESOURCE_PATH,
        vault_path);
    if(!ret.ok()) {
        THROW(ret.code(), ret.result());
    }

    char out_path[MAX_NAME_LEN];
    int status = setPathForGraftPathScheme(
                     const_cast<char*>(_destination_logical_path.c_str()),
                     vault_path.c_str(),
                     0, 0, 1, out_path);
    if(status < 0) {
        THROW(status, "setPathForGraftPathScheme failed");
    }

    return out_path;
} // make_physical_path

void register_data_object(
    rsComm_t*          _comm,
    rodsLong_t         _data_size,
    rodsLong_t         _resc_id,
    const std::string& _physical_path,
    const std::string& _logical_path,
    const std::string& _resource_hierarchy) {

    dataObjInfo_t obj_info;
    memset(&obj_info, 0, sizeof(obj_info));

    obj_info.dataSize = _data_size;
    obj_info.replStatus = 1;
    obj_info.rescId = _resc_id;
    rstrcpy(
        obj_info.objPath,
        _logical_path.c_str(),
        MAX_NAME_LEN);
    rstrcpy(
        obj_info.filePath,
        _physical_path.c_str(),
        MAX_NAME_LEN);
    rstrcpy(
        obj_info.rescHier,
        _resource_hierarchy.c_str(),
        MAX_NAME_LEN);
    rstrcpy(
        obj_info.dataType,
        "generic",
        NAME_LEN );
    rstrcpy(
        obj_info.dataMode,
        "0",
        SHORT_STR_LEN );
    rstrcpy(
        obj_info.dataOwnerName,
        _comm->clientUser.userName,
        NAME_LEN );
    rstrcpy(
        obj_info.dataOwnerZone,
        _comm->clientUser.rodsZone,
        NAME_LEN );

    int status = svrRegDataObj(_comm, &obj_info);
    if(status < 0) {
        std::string msg("failed to register object [");
        msg += _logical_path + "]";
        THROW(status, msg);
    }

} // register_data_object

void unlink_data_object(
    rsComm_t*          _comm,
    const std::string& _logical_path,
    const std::string& _resource_hierarchy) {

    dataObjInp_t obj_inp;
    memset(&obj_inp, 0, sizeof(obj_inp));

    obj_inp.oprType = UNREG_OPR;
    rstrcpy(
        obj_inp.objPath,
        _logical_path.c_str(),
        MAX_NAME_LEN);
    addKeyVal(
        &obj_inp.condInput,
        RESC_HIER_STR_KW,
        _resource_hierarchy.c_str());
    addKeyVal(
        &obj_inp.condInput,
        FORCE_FLAG_KW,
        "1" );

    int status = rsDataObjUnlink(_comm, &obj_inp);
    if(status < 0) {
        std::string msg("failed to unlink object [");
        msg += _logical_path + "]";
        THROW(status, msg);
    }

} // unlink_data_object

void update_object_size_and_phypath(
    rsComm_t*          _comm,
    rodsLong_t         _data_size,
    const std::string& _physical_path,
    const std::string& _logical_path,
    const std::string& _resource_hierarchy) {

    dataObjInfo_t obj_info;
    memset(&obj_info, 0, sizeof(obj_info));

    obj_info.dataSize = _data_size;
    rstrcpy(
        obj_info.objPath,
        _logical_path.c_str(),
        MAX_NAME_LEN);
    rstrcpy(
        obj_info.filePath,
        _physical_path.c_str(),
        MAX_NAME_LEN);
    rstrcpy(
        obj_info.rescHier,
        _resource_hierarchy.c_str(),
        MAX_NAME_LEN);

    keyValPair_t key_val;
    memset(&key_val, 0, sizeof(key_val));

    std::stringstream data_size_str;
    data_size_str << _data_size;
    addKeyVal(
        &key_val,
        DATA_SIZE_KW,
        data_size_str.str().c_str());

    addKeyVal(
        &key_val,
        FILE_PATH_KW,
        _physical_path.c_str());

    addKeyVal(
        &key_val,
        RESC_HIER_STR_KW,
        _resource_hierarchy.c_str());

    modDataObjMeta_t mod_obj_meta;
    memset(&mod_obj_meta, 0, sizeof(mod_obj_meta));
    mod_obj_meta.regParam    = &key_val;
    mod_obj_meta.dataObjInfo = &obj_info;

    int inx = rsModDataObjMeta(_comm, &mod_obj_meta);
    if(inx < 0) {
        std::stringstream msg;
        msg << "rsModDataObjMeta failed for [";
        msg << _logical_path;
        msg << "] : " << inx;
        THROW(inx, msg.str());
    }

} // update_object_size_and_phypath

int64_t stat_part(
    rsComm_t*          _comm,
    rodsLong_t         _resc_id,
    const std::string& _phy_path,
    const std::string& _log_path,
    const std::string& _resc_hier,
    const std::string& _host_name) {
    std::cout << " p: " << _phy_path
              << " l: " << _log_path
              << " h: " << _resc_hier
              << " n: " << _host_name
              << std::endl;

    fileStatInp_t f_inp;
    memset( &f_inp, 0, sizeof( f_inp ) );
    f_inp.rescId = _resc_id;
    rstrcpy(
        f_inp.fileName,
        _phy_path.c_str(),
        MAX_NAME_LEN );
    rstrcpy(
        f_inp.rescHier,
        _resc_hier.c_str(),
        MAX_NAME_LEN );
    rstrcpy(
        f_inp.objPath,
        _log_path.c_str(),
        MAX_NAME_LEN );
    rstrcpy(
        f_inp.addr.hostAddr,
        _host_name.c_str(),
        NAME_LEN );
    
    rodsStat_t* stbuf = nullptr;
    int status = rsFileStat( _comm, &f_inp, &stbuf );
    if ( status < 0 ) {
        std::stringstream msg;
        msg << "can not stat [" << f_inp.fileName << "]";
        THROW(status, msg.str());
    }

    return stbuf->st_size;
} // stat_part

bool stat_parts_and_update_catalog(
        rsComm_t*                        _comm,
        const irods::multipart_response& _mp_resp) {
    bool ret_val = true;
    try {
        for(auto& p : _mp_resp.parts) {
            // determine leaf resource
            irods::hierarchy_parser hp;
            hp.set_string(p.resource_hierarchy);
            std::string leaf_name;
            hp.last_resc(leaf_name);

            // resolve resource for leaf
            irods::resource_ptr resc;
            irods::error ret = resc_mgr.resolve(
                                   leaf_name,
                                   resc);
            if(!ret.ok()) {
                THROW(ret.code(), ret.result());
            }

            // fetch the resc id
            rodsLong_t resc_id = 0; 
            ret = resc->get_property<rodsLong_t>(
                    irods::RESOURCE_ID,
                    resc_id );
            if(!ret.ok()) {
                THROW(ret.code(), ret.result());
            }

            rodsLong_t file_size = stat_part(
                                       _comm,
                                       resc_id,
                                       p.destination_physical_path,
                                       p.destination_logical_path,
                                       p.resource_hierarchy,
                                       p.host_name);
            // was the part successfully transferred?
            if(file_size != p.file_size) {
                ret_val = false;
            }

            update_object_size_and_phypath(
                _comm,
                file_size,
                p.destination_physical_path,
                p.destination_logical_path,
                p.resource_hierarchy);
        } // for
    }
    catch(const irods::exception& _e) {
        std::cerr << _e.what() << std::endl;
        throw;
    }
    
    return ret_val;

} // stat_parts_and_update_catalog

void remove_multipart_collection(
    rsComm_t*          _comm,
    const std::string& _log_path) {
    namespace bfs = boost::filesystem;

    bfs::path path(_log_path);

    bfs::path p_path = path.parent_path();
    collInp_t coll_inp;
    memset(&coll_inp, 0, sizeof(coll_inp));
    rstrcpy(
        coll_inp.collName,
        p_path.string().c_str(),
        MAX_NAME_LEN);

    addKeyVal(
        &coll_inp.condInput,
        RECURSIVE_OPR__KW,
        "1");
    addKeyVal(
        &coll_inp.condInput,
        FORCE_FLAG_KW,
        "1");

    collOprStat_t* stat = nullptr;
    int status = rsRmColl(_comm, &coll_inp, &stat);
    if(status < 0) {
        THROW(status, "rsRmColl failed");
    }
} // remove_multipart_collection

void reassemble_part_objects(
    rsComm_t*                        _comm,
    const irods::multipart_response& _mp_resp) {
    try {
        // determine leaf resource
        irods::hierarchy_parser hp;
        hp.set_string(_mp_resp.parts.begin()->resource_hierarchy);
        std::string leaf_name;
        hp.last_resc(leaf_name);

        // resolve resource for leaf
        irods::resource_ptr resc;
        irods::error ret = resc_mgr.resolve(
                               leaf_name,
                               resc);
        if(!ret.ok()) {
            THROW(ret.code(), ret.result());
        }

        // fetch the resc id
        rodsLong_t resc_id = 0; 
        ret = resc->get_property<rodsLong_t>(
                irods::RESOURCE_ID,
                resc_id );
        if(!ret.ok()) {
            THROW(ret.code(), ret.result());
        }

        // use resc hier of first part, as all were repaved the same
        std::string dest_phy_path = make_physical_path(
                                        _mp_resp.parts.begin()->resource_hierarchy,
                                        _mp_resp.destination_logical_path);
        // build fileobj for dest
        irods::file_object_ptr dst_file_obj( new irods::file_object(
                                   _comm,
                                   _mp_resp.destination_logical_path,
                                   dest_phy_path,
                                   _mp_resp.parts.begin()->resource_hierarchy,
                                   0, getDefFileMode(), O_WRONLY | O_CREAT));

        // open dest phypath
        ret = resc->call(_comm, irods::RESOURCE_OP_OPEN, dst_file_obj);
        if(!ret.ok()) {
            THROW(ret.code(), ret.result());
        }

        // loop over parts
        for(auto& part : _mp_resp.parts) {
            // build fileobj for src part
            irods::file_object_ptr src_file_obj( new irods::file_object(
                                       _comm,
                                       part.destination_logical_path,
                                       part.destination_physical_path,
                                       part.resource_hierarchy,
                                       0, getDefFileMode(), O_RDONLY));

            // open source part
            ret = resc->call(_comm, irods::RESOURCE_OP_OPEN, src_file_obj);
            if(!ret.ok()) {
                THROW(ret.code(), ret.result());
            }

            bool done_flag = false;
            while(!done_flag) {
                // read from source part
                const ssize_t block_size = 4 * 1024 * 1024;
                uint8_t buff[block_size]; // TODO: parameterize
                ret = resc->call<void*,int>(
                          _comm,
                          irods::RESOURCE_OP_READ,
                          src_file_obj,
                          buff,
                          block_size);
                if(!ret.ok()) {
                    THROW(ret.code(), ret.result());
                }

                // bytes read is in the return code
                ssize_t read_size = ret.code();
                if(0 == read_size) {
                    break;
                }

                // write to dest part
                ret = resc->call<void*,int>(
                          _comm,
                          irods::RESOURCE_OP_WRITE,
                          dst_file_obj,
                          buff,
                          read_size);
                if(!ret.ok()) {
                    THROW(ret.code(), ret.result());
                }

            }// while

            // close source part
            ret = resc->call(
                    _comm,
                    irods::RESOURCE_OP_CLOSE,
                    src_file_obj);
            if(!ret.ok()) {
                THROW(ret.code(), ret.result());
            }

            // unlink part here
            unlink_data_object(
                _comm,
                part.destination_logical_path,
                part.resource_hierarchy);
        } // for

        // close destination part
        ret = resc->call(
                _comm,
                irods::RESOURCE_OP_CLOSE,
                dst_file_obj);
        if(!ret.ok()) {
            THROW(ret.code(), ret.result());
        }

        // update catalog for final data object
        rodsLong_t file_size = stat_part(
                                   _comm,
                                   resc_id,
                                   dest_phy_path,
                                   _mp_resp.destination_logical_path,
                                   _mp_resp.parts.begin()->resource_hierarchy,
                                   _mp_resp.parts.begin()->host_name);
        register_data_object(
            _comm,
            file_size,
            resc_id,
            dest_phy_path,
            _mp_resp.destination_logical_path,
            _mp_resp.parts.begin()->resource_hierarchy);

        // remove multipart collection
        remove_multipart_collection(
            _comm,
            _mp_resp.parts.begin()->destination_logical_path);
    }
    catch(const irods::exception& _e) {
        irods::log(LOG_ERROR, _e.what());
        throw;
    }

} // reassemble_part_objects
#endif

void transfer_executor_server(
    rsComm_t*          _comm,
    std::promise<int>* _port_promise) {
#ifdef RODS_SERVER
    typedef irods::message_broker::data_type data_t;
    
    std::string            part_phy_path;
    irods::unipart_request part_request;
    try {
        irods::message_broker bro("ZMQ_REP");

        // get the port range from server configuration
        const int start_port = irods::get_server_property<const int>(
                                   irods::CFG_SERVER_PORT_RANGE_START_KW);
        const int  end_port = irods::get_server_property<const int>(
                                  irods::CFG_SERVER_PORT_RANGE_END_KW);

        // bind to a port in the given range to handle the message passing
        int port = bro.bind_to_port_in_range(start_port, end_port);
        
        // set port promise value which notifies the calling thread
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
            avro::decode( *dec, part_request );

            if(part_request.transfer_complete) {
                bro.send(ACK_MSG);
                break;
            }

            // determine leaf resource
            irods::hierarchy_parser hp;
            hp.set_string(part_request.resource_hierarchy);
            std::string leaf_name;
            hp.last_resc(leaf_name);

            // resolve the leaf resource in question
            irods::resource_ptr resc;
            irods::error ret = resc_mgr.resolve(leaf_name, resc);
            if(!ret.ok()) {
                THROW(ret.code(), ret.result());
            }

            // create the file object fco
            irods::file_object_ptr file_obj( new irods::file_object(
                                       _comm,
                                       part_request.destination_logical_path,
                                       part_request.destination_physical_path,
                                       part_request.resource_hierarchy,
                                       0, getDefFileMode(), O_WRONLY | O_CREAT));

            // open the replica using the irods framework
            ret = resc->call(_comm, irods::RESOURCE_OP_OPEN, file_obj);
            if(!ret.ok()) {
                THROW(ret.code(), ret.result());
            }

            // acknowlege part request which initiates data transmission
            bro.send(ACK_MSG);

            // start writing things down
            bool done_flag = false;
            int64_t bytes_written = 0;
            while(!done_flag) {
                data_t rcv_data;
                bro.receive(rcv_data);

                // execute a write using the irods framework
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

                // acknowledge a successful write
                bro.send(ACK_MSG);

                // track number of bytes written
                bytes_written += rcv_data.size();
                if(bytes_written >= part_request.file_size) {
                    done_flag = true;
                }
            }// while

            // close object
            resc->call(_comm, irods::RESOURCE_OP_CLOSE, file_obj);
            if(!ret.ok()) {
                THROW(ret.code(), ret.result());
            }

        } // while

    }
    catch(const irods::exception& _e) {
        std::cerr << _e.what() << std::endl;
        // TODO: let thread exit cleanly?
        //throw;
    }
#endif
} // transfer_executor_server
    
void unipart_executor_server(
        irods::api_endpoint*  _endpoint ) {
#ifdef RODS_SERVER
    typedef irods::message_broker::data_type data_t;

    bool xfer_complete = false;
    irods::multipart_response mp_resp;
    try {
        rsComm_t* comm = nullptr;
        _endpoint->comm<rsComm_t*>(comm);

        // open the control channel back to the multipart server executor
        irods::message_broker cmd_skt("ZMQ_REP", _endpoint->ctrl_ctx());
        cmd_skt.connect("inproc://server_comms");

        // extract the payload
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

            // update the catalog?
            xfer_complete = stat_parts_and_update_catalog(comm, mp_resp);
    
        }
        else {
            // redirect, we are not the correct server
        }
    }
    catch(const zmq::error_t& _e) {
        irods::log(LOG_ERROR, _e.what());

        rsComm_t* comm = nullptr;
        _endpoint->comm<rsComm_t*>(comm);
        stat_parts_and_update_catalog(comm, mp_resp);

        _endpoint->done(true);
        THROW(SYS_SOCK_CONNECT_ERR, _e.what());
    }
    catch(const boost::bad_any_cast& _e) {
        // cannot update catalog, no mp_resp
        irods::log(LOG_ERROR, _e.what());
        _endpoint->done(true);
        THROW(INVALID_ANY_CAST, _e.what());
    }
    catch(const irods::exception& _e) {
        irods::log(LOG_ERROR, _e.what());

        rsComm_t* comm = nullptr;
        _endpoint->comm<rsComm_t*>(comm);
        stat_parts_and_update_catalog(comm, mp_resp);

        _endpoint->done(true);
        throw;
    }

    if(xfer_complete) {
        try {
            rsComm_t* comm = nullptr;
            _endpoint->comm<rsComm_t*>(comm);
            
            // if nothing has exploded, reassemble the parts
            reassemble_part_objects(comm, mp_resp);
        }
        catch(const irods::exception& _e) {
            irods::log(LOG_ERROR, _e.what());
            _endpoint->done(true);
            throw;
        }
    }

    _endpoint->done(true);

#endif
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

