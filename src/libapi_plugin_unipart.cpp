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

#ifdef RODS_SERVER
#include "miscServerFunct.hpp"
#endif

#include "irods_stacktrace.hpp"
#include "irods_server_api_call.hpp"
#include "irods_re_serialization.hpp"
#include "irods_api_envelope.hpp"
#include "irods_api_endpoint.hpp"
#include "irods_virtual_path.hpp"
#include "irods_resource_manager.hpp"
#include "irods_resource_backport.hpp"
#include "irods_file_object.hpp"
#include "irods_local_multipart_file.hpp"

//#include "irods_hostname.hpp"
bool hostname_resolves_to_local_address(const char* hostname);
extern irods::resource_manager resc_mgr;

#include "irods_multipart_avro_types.hpp"
#include "irods_unipart_request.hpp"
#include "data_transfer.hpp"
#include "unipart_api_endpoint.hpp"

#include "boost/lexical_cast.hpp"
#include "boost/filesystem.hpp"
#include "boost/filesystem/operations.hpp"
#include "boost/filesystem/path.hpp"

// =-=-=-=-=-=-=-
// stl includes
#include <sstream>
#include <string>
#include <stdexcept>
#include <iostream>
#include <future>
#include <thread>


void print_client_context(
        const irods::client_transport_plugin_context& _context) {

    std::cout << "multipart_operation: " << _context.operation << std::endl;
    std::cout << "local_filepath: " << _context.local_filepath << std::endl;
    for(auto p : _context.parts) {
        std::cout << "host_name: " << p.host_name << std::endl;
        std::cout << "logical_path: " << p.logical_path << std::endl;
        std::cout << "physical_path: " << p.physical_path << std::endl;
        std::cout << "resource_hierarchy: " << p.resource_hierarchy << std::endl;
        std::cout << "start_offset: " << p.start_offset << std::endl;
        std::cout << "bytes_already_transferred: " << p.bytes_already_transferred << std::endl;
        std::cout << "file_size: " << p.part_size << std::endl;
    }
}

void print_server_context(
        const irods::server_transport_plugin_context& _context) {

    std::cout << "multipart_operation: " << _context.operation << std::endl;
    std::cout << "data_object_path: " << _context.data_object_path << std::endl;
    for(auto p : _context.parts) {
        std::cout << "host_name: " << p.host_name << std::endl;
        std::cout << "logical_path: " << p.logical_path << std::endl;
        std::cout << "physical_path: " << p.physical_path << std::endl;
        std::cout << "resource_hierarchy: " << p.resource_hierarchy << std::endl;
        std::cout << "start_offset: " << p.start_offset << std::endl;
        std::cout << "bytes_already_transferred: " << p.bytes_already_transferred << std::endl;
        std::cout << "file_size: " << p.part_size << std::endl;
    }
}

void transfer_executor_client(
    std::shared_ptr<irods::local_multipart_file> _mp_file,
    const int                                     _port,
    const std::string&                            _host_name,
    const std::string                             _cmd_conn_str,
    std::shared_ptr<zmq::context_t>               _zmq_ctx,
    const std::vector<irods::part_request>&       _part_queue,
    const irods::client_transport_plugin_context& _context) {

    auto impl = [&_context]() -> std::shared_ptr<irods::multipart_method> {
        switch(_context.operation) {
            case irods::multipart_operation_t::GET:
                return std::make_shared<irods::get>();
            case irods::multipart_operation_t::PUT:
                return std::make_shared<irods::put>();
        }
    }();

    try {
        irods::message_broker cmd_skt{irods::zmq_type::RESPONSE, {}, _zmq_ctx};
        cmd_skt.connect(_cmd_conn_str);

        irods::message_broker bro{irods::zmq_type::REQUEST, {.timeout = _context.timeout, .retries = _context.retries}};
        std::stringstream conn_sstr;
        conn_sstr << "tcp://" << _host_name << ":";
        conn_sstr << _port;
        bro.connect(conn_sstr.str());

        //bool quit_received_flag      = false;
        for(auto& part : _part_queue) {
            /*
            const auto cmd_rcv_msg = cmd_skt.receive(ZMQ_DONTWAIT);
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
            uni_req.part_size = part.part_size;
            uni_req.start_offset = part.start_offset;
            uni_req.bytes_already_transferred = part.bytes_already_transferred;
            uni_req.resource_hierarchy = part.resource_hierarchy;
            uni_req.logical_path = part.logical_path;
            uni_req.physical_path = part.physical_path;

            bro.send(uni_req);

            const auto ack_rcv_msg = bro.receive();
            if(ACK_MSG != ack_rcv_msg) {
                // TODO: process error here
            }

            ssize_t bytes_remaining = part.part_size - part.bytes_already_transferred;
            do {
                // handle possible incoming command requests
                /*
                const auto cmd_rcv_msg = cmd_skt.receive(ZMQ_DONTWAIT);
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
                bytes_remaining = impl->client_transfer( bro, _mp_file, uni_req, bytes_remaining);
            } while (bytes_remaining > 0);

            // if we got a quit while transporting a part, quit gracefully
            /*if(quit_received_flag) {
                break;
            }*/

        } // for
        impl->client_cleanup(bro);

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
        std::shared_ptr<irods::api_endpoint>  _ep_ptr ) {
    auto unipart_client_ep_ptr = std::dynamic_pointer_cast<irods::unipart_api_client_endpoint>(_ep_ptr);
    try {
        const auto& context = unipart_client_ep_ptr->context();

        // open the control channel back to the multipart client executor
        irods::message_broker cmd_skt{irods::zmq_type::RESPONSE, {}, unipart_client_ep_ptr->ctrl_ctx()};
        cmd_skt.connect("inproc://client_comms");

        // guarantee that all parts have same host_name and resource_hierarchy
        // as all parts route to the one resource for reassembly in this plugin
        for(auto& p : unipart_client_ep_ptr->context().parts) {
            p.host_name = context.parts.begin()->host_name;
            p.resource_hierarchy = context.parts.begin()->resource_hierarchy;
        }

        const size_t file_size = [&context]() {
            size_t acc{0};
            for (const auto& part : context.parts) {
                acc += part.part_size;
            }
            return acc;
        }();

        auto mp_file = std::make_shared<irods::local_multipart_file>(
                context.local_filepath,
                context.local_filepath, //TODO: make this the data object name
                0,
                file_size,
                context.parts.size(),
                4 * 1024 * 1024,
                context.operation);

        // TODO: deal parts out in a serial fashion to cluster sequential reads in one thread
        // deal parts out to thread task queues
        std::vector<std::vector<irods::part_request>> part_queues(context.port_list.size());
        std::vector<std::vector<irods::part_request>>::iterator part_itr = part_queues.begin();
        for(size_t idx{0}; idx < context.parts.size(); ++idx) {
            part_itr->push_back(context.parts[idx]);
            part_itr->back().bytes_already_transferred = mp_file->get_bytes_already_transferred_for_part(
                    idx,
                    part_itr->back().part_size,
                    part_itr->back().bytes_already_transferred,
                    context.operation);
            if (part_itr->back().bytes_already_transferred == part_itr->back().part_size) {
                part_itr->pop_back();
                continue;
            }
            part_itr++;
            if(part_queues.end() == part_itr) {
                part_itr = part_queues.begin();
            }
        }

        auto xport_zmq_ctx = std::make_shared<zmq::context_t>(context.parts.size()+1);

        // TODO: create vector of message brokers from local context
        std::vector<std::unique_ptr<irods::message_broker>> xport_skts{};

        std::vector<std::thread> threads{};

        const irods::broker_settings settings{};
        for(size_t tid = 0; tid <context.port_list.size(); ++tid) {
            xport_skts.emplace_back(std::make_unique<irods::message_broker>(
                        irods::zmq_type::REQUEST, settings, xport_zmq_ctx));
            std::stringstream conn_sstr;
            conn_sstr << "inproc://xport_client_to_executors_";
            conn_sstr << tid;
            (*xport_skts.rbegin())->bind(conn_sstr.str());

            // TODO: pass local context to thread for ipc broker
            threads.emplace_back(
                transfer_executor_client,
                mp_file,
                context.port_list[tid].port,
                context.port_list[tid].host_name,
                conn_sstr.str(),
                xport_zmq_ctx,
                part_queues[tid],
                context);
        } // for thread id

        /*bool quit_received_flag = false;
        // receive and process commands
        while(true) {
            const auto cli_msg = cmd_skt.receive(ZMQ_DONTWAIT);

            // forward message from multipart client to transport threads
            for(auto& skt : xport_skts) {
                skt->send(cli_msg);
                const auto rcv_msg = skt->receive();
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
            threads[tid].join();
        }
        mp_file->finalize();
    }
    catch(const zmq::error_t& _e) {
        //TODO: notify client of failure
        std::cerr << _e.what() << std::endl;
        unipart_client_ep_ptr->done(true);
        THROW(SYS_SOCK_CONNECT_ERR, _e.what());
    }
    catch(const boost::bad_any_cast& _e) {
        //TODO: notify client of failure
        std::cerr << _e.what() << std::endl;
        unipart_client_ep_ptr->done(true);
        THROW(INVALID_ANY_CAST, _e.what());
    }
    catch(const irods::exception& _e) {
        //TODO: notify client of failure
        std::cerr << _e.what() << std::endl;
        unipart_client_ep_ptr->done(true);
        throw;
    }

    unipart_client_ep_ptr->done(true);

} // unipart_executor_client

#ifdef RODS_SERVER
irods::resource_ptr get_leaf_resource(const std::string& _resource_hierarchy) {
    // determine leaf resource
    irods::hierarchy_parser hp;
    hp.set_string(_resource_hierarchy);
    std::string leaf_name;
    hp.last_resc(leaf_name);

    // resolve the leaf resource in question
    irods::resource_ptr resc;
    irods::error ret = resc_mgr.resolve(leaf_name, resc);
    if(!ret.ok()) {
        THROW(ret.code(), ret.result());
    }
    return resc;
}

std::string make_physical_path(
    const std::string& _resource_hierarchy,
    const std::string& _destination_logical_path) {

    const auto leaf_resc = get_leaf_resource(_resource_hierarchy);

    std::string vault_path;
    const auto ret = leaf_resc->get_property<std::string>(
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

    dataObjInp_t obj_inp{};

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

int64_t stat_file(
    rsComm_t*          _comm,
    rodsLong_t         _resc_id,
    const std::string& _phy_path,
    const std::string& _log_path,
    const std::string& _resc_hier,
    const std::string& _host_name) {

    /*
    std::cout << " p: " << _phy_path
              << " l: " << _log_path
              << " h: " << _resc_hier
              << " n: " << _host_name
              << std::endl;
    */

    fileStatInp_t f_inp{};
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
} // stat_file

bool stat_parts_and_update_catalog(
        rsComm_t*                        _comm,
        const irods::server_transport_plugin_context& _context) {
    bool ret_val = true;
    try {
        for(auto& p : _context.parts) {
            const auto resc = get_leaf_resource(p.resource_hierarchy);

            // fetch the resc id
            rodsLong_t resc_id = 0;
            const auto ret = resc->get_property<rodsLong_t>(
                    irods::RESOURCE_ID,
                    resc_id );
            if(!ret.ok()) {
                THROW(ret.code(), ret.result());
            }

            rodsLong_t file_size = stat_file(
                                       _comm,
                                       resc_id,
                                       p.physical_path,
                                       p.logical_path,
                                       p.resource_hierarchy,
                                       p.host_name);
            // was the part successfully transferred?
            rodsLog(LOG_NOTICE, "file_size: %ju, part_size: %ju", static_cast<uintmax_t>(file_size), static_cast<uintmax_t>(p.part_size));
            if(file_size != p.part_size) {
                ret_val = false;
            }

            update_object_size_and_phypath(
                _comm,
                file_size,
                p.physical_path,
                p.logical_path,
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
    const irods::server_transport_plugin_context& _context) {
    try {
        const auto resc = get_leaf_resource(_context.parts.begin()->resource_hierarchy);

        // fetch the resc id
        rodsLong_t resc_id = 0;
        const auto get_resc_id_ret = resc->get_property<rodsLong_t>(
                irods::RESOURCE_ID,
                resc_id );
        if(!get_resc_id_ret.ok()) {
            THROW(get_resc_id_ret.code(), get_resc_id_ret.result());
        }

        // use resc hier of first part, as all were repaved the same
        std::string dest_phy_path = make_physical_path(
                                        _context.parts.begin()->resource_hierarchy,
                                        _context.data_object_path);
        // build fileobj for dest
        irods::file_object_ptr dst_file_obj( new irods::file_object(
                                   _comm,
                                   _context.data_object_path,
                                   dest_phy_path,
                                   _context.parts.begin()->resource_hierarchy,
                                   0, getDefFileMode(), O_WRONLY | O_CREAT));

        // open dest phypath
        const auto open_dst_ret = fileOpen(dst_file_obj->comm(), dst_file_obj);
        if(!open_dst_ret.ok()) {
            THROW(open_dst_ret.code(), open_dst_ret.result());
        }

        // loop over parts
        for(auto& part : _context.parts) {
            // build fileobj for src part
            irods::file_object_ptr src_file_obj( new irods::file_object(
                                       _comm,
                                       part.logical_path,
                                       part.physical_path,
                                       part.resource_hierarchy,
                                       0, getDefFileMode(), O_RDONLY));

            // open source part
            const auto open_src_ret = fileOpen(src_file_obj->comm(), src_file_obj);
            if(!open_src_ret.ok()) {
                THROW(open_src_ret.code(), open_src_ret.result());
            }

            bool done_flag = false;
            while(!done_flag) {
                // read from source part
                const ssize_t block_size = 4 * 1024 * 1024;
                uint8_t buff[block_size]; // TODO: parameterize
            struct stat stat_for_file_obj{};
            fileStat(src_file_obj->comm(), src_file_obj, &stat_for_file_obj);
                const auto read_ret = fileRead(
                        src_file_obj->comm(),
                        src_file_obj,
                        buff,
                        block_size);
                if(!read_ret.ok()) {
                    THROW(read_ret.code(), read_ret.result());
                }

                // bytes read is in the return code
                ssize_t read_size = read_ret.code();
                if(0 == read_size) {
                    break;
                }

                // write to dest part
                const auto write_ret = fileWrite(
                        dst_file_obj->comm(),
                        dst_file_obj,
                        buff,
                        read_size);
                if(!write_ret.ok()) {
                    THROW(write_ret.code(), write_ret.result());
                }

            }// while

            // close source part
            const auto close_src_ret = fileClose(
                    src_file_obj->comm(),
                    src_file_obj);
            if(!close_src_ret.ok()) {
                THROW(close_src_ret.code(), close_src_ret.result());
            }

            // unlink part here
            unlink_data_object(
                _comm,
                part.logical_path,
                part.resource_hierarchy);
        } // for

        // close destination part
        const auto close_dst_ret = fileClose(
                dst_file_obj->comm(),
                dst_file_obj);
        if(!close_dst_ret.ok()) {
            THROW(close_dst_ret.code(), close_dst_ret.result());
        }

        // update catalog for final data object
        rodsLong_t file_size = stat_file(
                                   _comm,
                                   resc_id,
                                   dest_phy_path,
                                   _context.data_object_path,
                                   _context.parts.begin()->resource_hierarchy,
                                   _context.parts.begin()->host_name);
        register_data_object(
            _comm,
            file_size,
            resc_id,
            dest_phy_path,
            _context.data_object_path,
            _context.parts.begin()->resource_hierarchy);

        // remove multipart collection
        remove_multipart_collection(
            _comm,
            _context.parts.begin()->logical_path);
    }
    catch(const irods::exception& _e) {
        irods::log(LOG_ERROR, _e.what());
        throw;
    }

} // reassemble_part_objects
#endif

void transfer_executor_server(
    rsComm_t*                                     _comm,
    std::promise<int>*                            _port_promise,
    const irods::server_transport_plugin_context& _context) {
#ifdef RODS_SERVER

    auto impl = [&_context]() -> std::shared_ptr<irods::multipart_method> {
        switch(_context.operation) {
            case irods::multipart_operation_t::GET:
                return std::make_shared<irods::get>();
            case irods::multipart_operation_t::PUT:
                return std::make_shared<irods::put>();
        }
    }();

    try {
        irods::message_broker bro{irods::zmq_type::RESPONSE, {.timeout = _context.timeout, .retries = _context.retries}};

        // get the port range from server configuration
        const int start_port = irods::get_server_property<const int>(
                                   irods::CFG_SERVER_PORT_RANGE_START_KW);
        const int  end_port = irods::get_server_property<const int>(
                                  irods::CFG_SERVER_PORT_RANGE_END_KW);

        // bind to a port in the given range to handle the message passing
        const int port = bro.bind_to_port_in_range(start_port, end_port);

        // set port promise value which notifies the calling thread
        _port_promise->set_value(port);

        while( true ) { // TODO: add timeout?

            // wait for next part request
            const auto uni_req = bro.receive<irods::unipart_request>();

            // acknowlege part request which initiates data transmission if transfer incomplete
            bro.send(ACK_MSG);

            if(uni_req.transfer_complete) {
                break;
            }

            ssize_t bytes_remaining = uni_req.part_size - uni_req.bytes_already_transferred;
            const size_t block_size = 4 * 1024 * 1024;
            // start writing things down
            do {
                bytes_remaining = impl->server_transfer(_comm, bro, uni_req, bytes_remaining, block_size);
            } while (bytes_remaining > 0);

        } // while

    }
    catch(const irods::exception& _e) {
        std::cerr << _e.what() << std::endl;
        // TODO: let thread exit cleanly?
        //throw;
    }
#endif
} // transfer_executor_server

bool server_executor_impl(
    rsComm_t*                  _comm,
    irods::message_broker&     _cmd_skt,
    std::shared_ptr<irods::unipart_api_server_endpoint> _unipart_server_ep_ptr) {
#ifdef RODS_SERVER

    const auto& context = _unipart_server_ep_ptr->context();
    std::vector<std::thread> threads{};
    std::vector<std::promise<int>>            promises(context.number_of_threads);
    irods::socket_address_list open_socket_list;

    // start threads to receive part data
    for(int tid = 0; tid < context.number_of_threads; ++tid) {
        threads.emplace_back(
                transfer_executor_server,
                _comm,
                &promises[tid],
                context);
    }

    // wait until all ports are bound and fill in
    // the ports in the response object
    for( int pid = 0; pid < context.number_of_threads; ++pid) {
        auto f = promises[pid].get_future();
        f.wait();
        irods::socket_address sckt_addr;
        sckt_addr.port = f.get();
        sckt_addr.host_name = context.host_name;
        open_socket_list.sockets.push_back(sckt_addr);
    }

    const auto rcv_msg = _cmd_skt.receive();

    // respond with the object
    _cmd_skt.send(open_socket_list);

    // wait for all threads to complete
    for( size_t tid = 0; tid < threads.size(); ++tid) {
        threads[tid].join();
    }

    // update the catalog?
    switch(context.operation) {
        case irods::multipart_operation_t::PUT:
            return stat_parts_and_update_catalog(_comm, context);
        case irods::multipart_operation_t::GET:
            return true;
    }
#else
    return false;
#endif
} // server_executor_impl

void unipart_executor_server(
        std::shared_ptr<irods::api_endpoint>  _ep_ptr ) {
    auto unipart_server_ep_ptr = std::dynamic_pointer_cast<irods::unipart_api_server_endpoint>(_ep_ptr);
#ifdef RODS_SERVER
    try {
        if(unipart_server_ep_ptr->context().number_of_threads <= 0) {
            THROW(SYS_INVALID_INPUT_PARAM, "invalid number of threads");
        }

        const auto& context = unipart_server_ep_ptr->context();
        auto comm = unipart_server_ep_ptr->comm<rsComm_t*>();

        // open the control channel back to the multipart server executor
        irods::message_broker cmd_skt(irods::zmq_type::RESPONSE, {}, unipart_server_ep_ptr->ctrl_ctx());
        cmd_skt.connect("inproc://server_comms");

        const std::string& host_name = context.host_name;
        if(hostname_resolves_to_local_address(host_name.c_str())) {
            if(server_executor_impl(comm, cmd_skt, unipart_server_ep_ptr) && irods::multipart_operation_t::PUT == context.operation) {
                reassemble_part_objects(comm, context);
            }
        } else {
            // redirect, we are not the correct server
            int remote_flag = 0;
            rodsServerHost_t* server_host = nullptr;
            irods::error ret = irods::get_host_for_hier_string(
                                   context.parts.begin()->resource_hierarchy,
                                   remote_flag,
                                   server_host);
            if(!ret.ok()) {
                THROW(ret.code(), ret.result());
            }

            int status = svrToSvrConnect(comm, server_host);
            if(status < 0) {
                THROW(status, "svrToSvrConnect failed for ");
            }

            irods::api_envelope envelope;
            envelope.endpoint_name = "api_plugin_unipart"; // TODO: magic string
            envelope.connection_type = irods::API_EP_SERVER_TO_SERVER;
            envelope.payload.clear();

            auto p_out = avro::memoryOutputStream();
            auto p_enc = avro::binaryEncoder();
            p_enc->init( *p_out );
            avro::encode( *p_enc, context );
            auto p_data = avro::snapshot( *p_out );

            envelope.payload.resize(p_data->size());
            envelope.payload.assign(p_data->begin(), p_data->end());

            auto e_out = avro::memoryOutputStream();
            auto e_enc = avro::binaryEncoder();
            e_enc->init( *e_out );
            avro::encode( *e_enc, envelope );
            auto e_data = avro::snapshot( *e_out );

            bytesBuf_t inp;
            memset(&inp, 0, sizeof(bytesBuf_t));
            inp.len = e_data->size();
            inp.buf = e_data->data();

            void *tmp_out = NULL;
            status = procApiRequest(
                             server_host->conn,
                             5000, &inp, NULL,
                             &tmp_out, NULL );
            if ( status < 0 ) {
                THROW(status, "v5 API failed");
            }
            else {
                if ( tmp_out != NULL ) {
                    portalOprOut_t* portal = static_cast<portalOprOut_t*>( tmp_out );

                   // TODO: we need to bridge the server_comms inproc socket to the
                   // remote socket we are creating here
                    irods::message_broker rem_bro{irods::zmq_type::REQUEST, {.timeout = context.timeout, .retries = context.retries}};
                    std::stringstream conn_sstr;
                    conn_sstr << "tcp://" << context.parts.begin()->host_name << ":";
                    conn_sstr << portal->portList.portNum;
                    rem_bro.connect(conn_sstr.str());

                    rem_bro.send(cmd_skt.receive());
                    cmd_skt.send(rem_bro.receive());

                    rcOprComplete(server_host->conn, 0);
                }
                else {
                    printf( "ERROR: the 'out' variable is null\n" );
                }
            }

            rcDisconnect(server_host->conn);

        } // else
    }
    catch(const boost::bad_any_cast& _e) {
        // cannot update catalog, no mp_resp
        irods::log(LOG_ERROR, _e.what());
        unipart_server_ep_ptr->done(true);
        THROW(INVALID_ANY_CAST, _e.what());
    }
    catch(const irods::exception& _e) {
        irods::log(LOG_ERROR, _e.what());

        auto comm = unipart_server_ep_ptr->comm<rsComm_t*>();
        stat_parts_and_update_catalog(comm, unipart_server_ep_ptr->context());

        unipart_server_ep_ptr->done(true);
        throw;
    }

    unipart_server_ep_ptr->done(true);

#endif
} // unipart_executor_server

void unipart_executor_server_to_server(
        std::shared_ptr<irods::api_endpoint>  _ep_ptr ) {
    auto unipart_server_ep_ptr = std::dynamic_pointer_cast<irods::unipart_api_server_endpoint>(_ep_ptr);
#ifdef RODS_SERVER
    try {
        const auto& context = unipart_server_ep_ptr->context();

        irods::message_broker bro{irods::zmq_type::RESPONSE, {.timeout = context.timeout, .retries = context.retries}};
        const int start_port = irods::get_server_property<const int>(
                                   irods::CFG_SERVER_PORT_RANGE_START_KW);
        const int  end_port = irods::get_server_property<const int>(
                                  irods::CFG_SERVER_PORT_RANGE_END_KW);
        const int port = bro.bind_to_port_in_range(start_port, end_port);
        unipart_server_ep_ptr->port(port);

        if(context.parts.empty()) {
            THROW(SYS_INVALID_INPUT_PARAM, "empty parts array");
        }

        if(context.number_of_threads <= 0) {
            THROW(SYS_INVALID_INPUT_PARAM, "invalid number of threads");
        }

        auto comm = unipart_server_ep_ptr->comm<rsComm_t*>();

        // guarantee that all parts have same host_name and resource_hierarchy
        // for this plugin all parts route as single resource for reassembly
        for(auto& p : unipart_server_ep_ptr->context().parts) {
            p.host_name = context.parts.begin()->host_name;
            p.resource_hierarchy = context.parts.begin()->resource_hierarchy;
        }

        // by convention choose the first resource as our target
        const std::string& host_name = context.parts.begin()->host_name;
        if(hostname_resolves_to_local_address(host_name.c_str())) {
            // open the control channel back to the unipart server executor
#if 1
            if(server_executor_impl(comm, bro, unipart_server_ep_ptr) && irods::multipart_operation_t::PUT == context.operation) {
                reassemble_part_objects(comm, unipart_server_ep_ptr->context());
            }
#endif
        }
        else {
            // TODO: should not get here
        }
        //rcDisconnect(server_host->conn);
    }
    catch(const boost::bad_any_cast& _e) {
        // cannot update catalog, no mp_resp
        irods::log(LOG_ERROR, _e.what());
        unipart_server_ep_ptr->done(true);
        THROW(INVALID_ANY_CAST, _e.what());
    }
    catch(const irods::exception& _e) {
        irods::log(LOG_ERROR, _e.what());
        unipart_server_ep_ptr->done(true);
        throw;
    }

    unipart_server_ep_ptr->done(true);
#endif
} // unipart_executor_server_to_server

void irods::unipart_api_endpoint::capture_executors(
        irods::api_endpoint::thread_executor& _cli,
        irods::api_endpoint::thread_executor& _svr,
        irods::api_endpoint::thread_executor& _svr_to_svr) {
    _cli        = unipart_executor_client;
    _svr        = unipart_executor_server;
    _svr_to_svr = unipart_executor_server_to_server;
}

int irods::unipart_api_endpoint::status(rError_t* _err) const {
    if(status_ < 0) {
        addRErrorMsg(
                _err,
                status_,
                error_message_.str().c_str());
    }
    return status_;
}

void irods::unipart_api_client_endpoint::initialize_from_context(const std::vector<uint8_t>& _bytes) {
    set_context_from_bytes(_bytes);
}

void irods::unipart_api_server_endpoint::initialize_from_context(const std::vector<uint8_t>& _bytes) {
    set_context_from_bytes(_bytes);
}

extern "C" {
    irods::api_endpoint* plugin_factory(
            const std::string&,     //_inst_name
            const irods::connection_t& _connection_type ) { // _context
        switch(_connection_type) {
            case irods::API_EP_CLIENT:
                return new irods::unipart_api_client_endpoint(_connection_type);
            case irods::API_EP_SERVER:
            case irods::API_EP_SERVER_TO_SERVER:
                return new irods::unipart_api_server_endpoint(_connection_type);
        }
    }
};

