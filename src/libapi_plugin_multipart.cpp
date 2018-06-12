// =-=-=-=-=-=-=-
// irods includes
#include "apiHandler.hpp"
#include "rodsPackInstruct.h"
#include "objStat.h"
#include "physPath.hpp"
#include "rcMisc.h"
#include "rsGenQuery.hpp"
#include "rsCollCreate.hpp"
#include "rsDataObjCreate.hpp"
#include "rsDataObjClose.hpp"
#include "rsObjStat.hpp"

#include "irods_stacktrace.hpp"
#include "irods_server_api_call.hpp"
#include "irods_re_serialization.hpp"
#include "irods_api_envelope.hpp"
#include "irods_api_endpoint.hpp"
#include "irods_virtual_path.hpp"
#include "irods_resource_manager.hpp"
#include "irods_resource_redirect.hpp"
#include "irods_hierarchy_parser.hpp"

#include "irods_multipart_avro_types.hpp"
#include "multipart_api_endpoint.hpp"

#include "boost/lexical_cast.hpp"
#include "boost/filesystem/operations.hpp"
#include "boost/filesystem/path.hpp"
#include "boost/filesystem.hpp"
#include "boost/format.hpp"
#include "boost/algorithm/string/predicate.hpp"

// =-=-=-=-=-=-=-
// stl includes
#include <set>
#include <sstream>
#include <string>
#include <iostream>

extern irods::resource_manager resc_mgr;

static const irods::message_broker::data_type REQ_MSG = {'R', 'E', 'Q'};
static const irods::message_broker::data_type ACK_MSG = {'A', 'C', 'K'};
static const irods::message_broker::data_type QUIT_MSG = {'q', 'u', 'i', 't'};
static const irods::message_broker::data_type PROG_MSG = {'p', 'r', 'o', 'g', 'r', 'e', 's', 's'};
static const irods::message_broker::data_type FINALIZE_MSG = {'F', 'I', 'N', 'A', 'L', 'I', 'Z', 'E'};
static const irods::message_broker::data_type ERROR_MSG = {'e', 'r', 'r', 'o', 'r'};

namespace po = boost::program_options;

void print_mp_response(
        const irods::multipart_response& _resp) {

    std::cout << " number of threads: " << _resp.port_list.size() << std::endl;

    for(auto p : _resp.port_list) {
        std::cout << "socket: " << p.host_name << ":" << p.port << std::endl;
    }

    for(auto p : _resp.parts) {
        std::cout << "host_name: " << p.host_name << std::endl;
        std::cout << "logical_path: " << p.logical_path << std::endl;
        std::cout << "physical_path: " << p.physical_path << std::endl;
        std::cout << "resource_hierarchy: " << p.resource_hierarchy << std::endl;
        std::cout << "start_offset: " << p.start_offset << std::endl;
        std::cout << "bytes_already_transferred: " << p.bytes_already_transferred << std::endl;
    }
}

static std::string construct_data_object_path(
        const std::string& _phy_path,
        const std::string& _data_object_path) {
    namespace bfs = boost::filesystem;

    return boost::ends_with(_data_object_path, irods::get_virtual_path_separator()) ?
        _data_object_path + bfs::path{_phy_path}.filename().string() :
        _data_object_path;

} // construct_data_object_path

void multipart_executor_client(
        std::shared_ptr<irods::api_endpoint>  _ep_ptr ) {
    namespace bfs = boost::filesystem;

    auto multipart_ep_ptr = std::dynamic_pointer_cast<irods::multipart_api_client_endpoint>(_ep_ptr);
    try {
        rcComm_t* comm = multipart_ep_ptr->comm<rcComm_t*>();

        const auto& mp_req = multipart_ep_ptr->request();

        irods::message_broker client_cmd_skt{irods::zmq_type::RESPONSE, {}, multipart_ep_ptr->ctrl_ctx()};
        client_cmd_skt.connect("inproc://client_comms");

        // =-=-=-=-=-=-=-
        //TODO: parameterize
        irods::message_broker bro{irods::zmq_type::REQUEST, {.timeout = mp_req.timeout, .retries = mp_req.retries}};

        int port = multipart_ep_ptr->port();
        std::stringstream conn_sstr;
        conn_sstr << "tcp://localhost:";
        conn_sstr << port;
        bro.connect(conn_sstr.str());

        switch (mp_req.operation) {
            case irods::PUT: {
                bfs::path p(multipart_ep_ptr->context().local_filepath);

                if(!bfs::exists(p)) {
#if 0 // TODO - FIXME
                    bro->send(QUIT_MSG);
                    const auto = bro->receive();
#endif
                    multipart_ep_ptr->done(true);
                    return;
                }

                // send file size to server
                // TODO: can we do this without string conversion?
                off_t fsz = bfs::file_size(p);
                const auto fsz_str = boost::lexical_cast<std::string>(fsz);
                bro.send(fsz_str);

                break;
            }
            case irods::GET: {
                bfs::path p(multipart_ep_ptr->context().local_filepath);
                if(bfs::exists(p)) {
                    multipart_ep_ptr->done(true);
                    return;
                }

                bro.send(REQ_MSG);
                break;
            }
        }

        multipart_ep_ptr->response() = bro.receive<irods::multipart_response>();

        irods::client_transport_plugin_context transport_context;
        transport_context.local_filepath = multipart_ep_ptr->context().local_filepath;
        transport_context.operation = mp_req.operation;
        transport_context.parts = multipart_ep_ptr->response().parts;
        transport_context.port_list = multipart_ep_ptr->response().port_list;
        transport_context.timeout = multipart_ep_ptr->request().timeout;
        transport_context.retries = multipart_ep_ptr->request().retries;

        auto xport_ep_ptr = irods::create_command_object(multipart_ep_ptr->request().transport_mechanism, irods::API_EP_CLIENT);
        xport_ep_ptr->initialize_from_context(convert_to_bytes(transport_context));

        auto xport_zmq_ctx = std::make_shared<zmq::context_t>(1);
        irods::message_broker xport_cmd_skt{irods::zmq_type::REQUEST, {}, xport_zmq_ctx};
        xport_cmd_skt.bind("inproc://client_comms");

        irods::api_v5_to_v5_call_endpoint(
            comm,
            xport_ep_ptr,
            xport_zmq_ctx);

        // command and control message loop
        while(true) {
            const auto cli_msg = client_cmd_skt.receive(ZMQ_DONTWAIT);
            if(cli_msg.size()>0) {
                // forward message from client to transport control
                //xport_cmd_skt.send(cli_msg);
                //xport_cmd_skt.receive(rcv_msg); // TODO: process?

                if(QUIT_MSG == cli_msg) {
                    // forward quit from client to multipart instance
                    bro.send(cli_msg);
                    const auto rcv_msg = bro.receive();
                    if(ACK_MSG != rcv_msg) {
                        std::cerr << "client thread reported an error: " << rcv_msg << std::endl;
                        // TODO: process response?
                    }
                    client_cmd_skt.send(rcv_msg);
                    break;
                }

                //client_cmd_skt.send(rcv_msg);
            }
        } // while

        xport_ep_ptr->wait();

        multipart_ep_ptr->done(true);
    }
    catch(const zmq::error_t& _e) {
        std::cerr << _e.what() << std::endl;
        multipart_ep_ptr->done(true);
        THROW(SYS_SOCK_CONNECT_ERR, _e.what());
    }
    catch(const boost::bad_any_cast& _e) {
        std::cerr << _e.what() << std::endl;
        multipart_ep_ptr->done(true);
        THROW(INVALID_ANY_CAST, _e.what());
    }
    catch(const boost::bad_lexical_cast& _e) {
        std::cerr << _e.what() << std::endl;
        multipart_ep_ptr->done(true);
        THROW(INVALID_LEXICAL_CAST, _e.what());
    }
} // multipart_executor_client


#ifdef RODS_SERVER

static std::tuple<std::string, std::string> split_collection_and_object_paths(
        const std::string& _logical_path) {

    const std::string vps = irods::get_virtual_path_separator();

    return {_logical_path.substr(0, _logical_path.rfind(vps)),
            _logical_path.substr(_logical_path.rfind(vps) + 1)};
}

// =-=-=-=-=-=-=-
// TODO: externalize this to the rule engine
static std::string make_multipart_collection_path(
        const std::string& _logical_path) {

    std::string coll_path, obj_path;
    std::tie(coll_path, obj_path) = split_collection_and_object_paths(_logical_path);

    // =-=-=-=-=-=-=-
    // build logical collection path for query
    const auto& vps = irods::get_virtual_path_separator();
    return coll_path + vps + ".irods_" + obj_path + "_multipart" + vps;

} // make_multipart_collection_name

static bool query_for_restart(
    rsComm_t*                  _comm,
    const std::string&         _mp_coll,
    irods::multipart_response& _mp_resp ) {
    bool ret_val = false;

    // =-=-=-=-=-=-=-
    // determine if logical collection exists, get obj list
    const std::string mp_coll_without_trailing_vps = _mp_coll.substr(0, _mp_coll.size() - irods::get_virtual_path_separator().size());
    std::string query{"select DATA_NAME, DATA_PATH, DATA_SIZE, DATA_RESC_ID where COLL_NAME like '" + mp_coll_without_trailing_vps + "%'"};

    genQueryInp_t gen_inp{};
    fillGenQueryInpFromStrCond( (char*)query.c_str(), &gen_inp );

    gen_inp.maxRows = MAX_SQL_ROWS;
    gen_inp.continueInx = 0;

    genQueryOut_t* gen_out = nullptr;

    rodsLog(LOG_NOTICE, "%s", query.c_str());
    // =-=-=-=-=-=-=-
    // if obj list exists, build restart part array objs
    bool continue_flag = true;
    while( continue_flag ) {
        int status = rsGenQuery( _comm, &gen_inp, &gen_out );
        if(status < 0 ) {
            if(CAT_NO_ROWS_FOUND == status) {
                ret_val = !_mp_resp.parts.empty();
                break;
            }
            // TODO: report errors
            return false;
        }

        ret_val = true;
        sqlResult_t* name    = getSqlResultByInx( gen_out, COL_DATA_NAME );
        sqlResult_t* path    = getSqlResultByInx( gen_out, COL_D_DATA_PATH );
        sqlResult_t* size    = getSqlResultByInx( gen_out, COL_DATA_SIZE );
        sqlResult_t* resc_id = getSqlResultByInx( gen_out, COL_D_RESC_ID );
        for(auto i = 0; i < gen_out->rowCnt; ++i) {
            irods::part_request pr;
            pr.logical_path = _mp_coll + &name->value[name->len * i];
            pr.physical_path = &path->value[name->len * i];
            pr.bytes_already_transferred = std::stol(&size->value[size->len * i]);
            rodsLog(LOG_NOTICE, "bytes from reset = %ju", static_cast<uintmax_t>(pr.bytes_already_transferred));

            int id = std::stoi(&resc_id->value[resc_id->len * i]);
            resc_mgr.leaf_id_to_hier( id, pr.resource_hierarchy );
            _mp_resp.parts.push_back(pr);
        } // for i

        if(gen_out->continueInx > 0) {
            gen_inp.continueInx = gen_out->continueInx;
            freeGenQueryOut(&gen_out);
        }
        else {
            continue_flag = false;
        }

    } // while

    freeGenQueryOut(&gen_out);

    return ret_val;

} // query_for_restart


size_t resolve_number_of_parts(
        const size_t _default,
        const off_t _file_size,
        const size_t _block_size) {
    // TODO: invoke policy here for num parts
    const auto max_parts = _file_size / _block_size + !!(_file_size % _block_size);
    return std::min(_default, max_parts);

} // resolve_number_of_parts

void resolve_part_object_paths(
    const std::string&         _dst_coll,
    irods::multipart_response& _resp ) {
    if(_resp.parts.empty()) {
        THROW(SYS_INVALID_INPUT_PARAM, "empty parts");
    }

    namespace bfs = boost::filesystem;

    std::string prefix;
    const std::string vps = irods::get_virtual_path_separator();
    if( !boost::ends_with(_dst_coll, vps)) {
        prefix += vps;
    }

    size_t part_number = 0;
    for(auto& p : _resp.parts) {
        std::stringstream ss; ss << part_number;
        p.logical_path =
            _dst_coll +
            prefix +
            ".irods_part_" +
            ss.str();
        ++part_number;
    }
} // resolve_part_object_paths

void resolve_part_sizes(
    off_t                             _src_size,
    size_t                            _block_size,
    std::vector<irods::part_request>& _parts) {
    if(_parts.empty()) {
        THROW(SYS_INVALID_INPUT_PARAM, "empty parts");
    }

    // compute total bytes left over
    const auto leftover_bytes = _src_size % _block_size;

    //compute total number of blocks to write
    const auto number_of_blocks = _src_size / _block_size + !!leftover_bytes;

    // compute evenly distributed blocks
    const auto even_blocks = number_of_blocks / _parts.size();

    // compute number of blocks left over
    auto leftover_blocks = number_of_blocks % _parts.size();

    off_t offset = 0;
    for(auto& p : _parts) {
        if ( leftover_blocks != 0 ) {
            p.part_size = (even_blocks + 1) * _block_size;
            leftover_blocks--;
        } else {
            p.part_size = even_blocks * _block_size;
        }

        p.start_offset += offset;
        offset += p.part_size;
    }

    //if there were leftover bytes, the last block is shorter. If there weren't,
    //the modulus here ensures we subtract zero.
    const auto last_block_short_by = (_block_size - leftover_bytes) % _block_size;
    _parts.rbegin()->part_size -= last_block_short_by;

} // resolve_part_sizes

size_t resolve_number_of_threads(
    const size_t  _req_num,
    const size_t _number_of_parts) {

    // TODO: call PEP to externalize decision making
    return std::min(_req_num, _number_of_parts);

} // resolve_number_of_threads

void resolve_part_hierarchies_for_put(
    rsComm_t*                  _comm,
    bool                       _single_server,
    const std::string&         _dst_resc,
    irods::multipart_response& _resp) {

    dataObjInp_t obj_inp;
    if(_single_server) {
        memset(&obj_inp, 0, sizeof(obj_inp));

        auto& p = _resp.parts[0];

        obj_inp.dataSize = p.part_size;
        rstrcpy(
            obj_inp.objPath,
            p.logical_path.c_str(),
            MAX_NAME_LEN);

        addKeyVal(
                &obj_inp.condInput,
                RESC_NAME_KW,
                _dst_resc.c_str());

        std::string hier;
        irods::error ret = irods::resolve_resource_hierarchy(
                               irods::CREATE_OPERATION,
                               _comm,
                               &obj_inp,
                               hier );
        if(!ret.ok()) {
            THROW(ret.code(), ret.result());
        }

        for(auto& p : _resp.parts) {
            p.resource_hierarchy = hier;
        } // for
    }
    else {
        for(auto& p : _resp.parts) {
            memset(&obj_inp, 0, sizeof(obj_inp));

            obj_inp.dataSize = p.part_size;
            rstrcpy(
                obj_inp.objPath,
                p.logical_path.c_str(),
                MAX_NAME_LEN);

            std::string hier;
            irods::error ret = irods::resolve_resource_hierarchy(
                                   irods::CREATE_OPERATION,
                                   _comm,
                                   &obj_inp,
                                   hier );
            if(!ret.ok()) {
                THROW(ret.code(), ret.result());
            }

            p.resource_hierarchy = hier;

        } // for
    }
} // resolve_part_hierarchies_for_put

void resolve_part_hierarchies_for_get(
    rsComm_t*                  _comm,
    const bool                 _single_server,
    const std::string&         _data_object_path,
    const std::string&         _dst_resc,
    irods::multipart_response& _resp) {

    if(_single_server) {
        dataObjInp_t obj_inp{};

        obj_inp.oprType = GET_OPR;

        rstrcpy(obj_inp.objPath, _data_object_path.c_str(), sizeof(obj_inp.objPath));

        addKeyVal(
            &obj_inp.condInput,
            RESC_NAME_KW,
            _dst_resc.c_str());

        std::string hier;
        irods::error ret = irods::resolve_resource_hierarchy(
                               irods::OPEN_OPERATION,
                               _comm,
                               &obj_inp,
                               hier );
        if(!ret.ok()) {
            THROW(ret.code(), ret.result());
        }

        for(auto& p : _resp.parts) {
            p.resource_hierarchy = hier;
        } // for
    }
    else {
        for(auto& p : _resp.parts) {
            dataObjInp_t obj_inp{};

            obj_inp.oprType = GET_OPR;

            rstrcpy(obj_inp.objPath, _data_object_path.c_str(), sizeof(obj_inp.objPath));

            addKeyVal(
                &obj_inp.condInput,
                RESC_NAME_KW,
                _dst_resc.c_str());

            std::string hier;
            irods::error ret = irods::resolve_resource_hierarchy(
                                   irods::OPEN_OPERATION,
                                   _comm,
                                   &obj_inp,
                                   hier );
            if(!ret.ok()) {
                THROW(ret.code(), ret.result());
            }

            p.resource_hierarchy = hier;

        } // for
    }
} // resolve_part_hierarchies_for_get

void resolve_part_hostnames(
    irods::multipart_response& _resp) {
    for(auto& p : _resp.parts) {
        // determine leaf resource
        irods::hierarchy_parser hp;
        hp.set_string(p.resource_hierarchy);
        std::string leaf_name;
        hp.last_resc(leaf_name);

        // resolve resource for leaf
        irods::resource_ptr leaf_resc;
        resc_mgr.resolve(leaf_name, leaf_resc);

        // extract the host from the resource
        std::string resc_host;
        leaf_resc->get_property<std::string>(
            irods::RESOURCE_LOCATION,
            resc_host);

        p.host_name = resc_host;
    }
} // resolve_part_hostnames

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

void resolve_part_physical_paths(
    irods::multipart_response& _resp) {
    for(auto& p : _resp.parts) {
        p.physical_path = make_physical_path(
                                          p.resource_hierarchy,
                                          p.logical_path);
    } // for
} // resolve_part_physical_paths

void create_multipart_collection(
    rsComm_t*          _comm,
    const std::string& _mp_coll) {

    collInp_t coll_inp{};

    addKeyVal(
        &coll_inp.condInput,
        RECURSIVE_OPR__KW, "");
    rstrcpy(
        coll_inp.collName,
        _mp_coll.c_str(),
        MAX_NAME_LEN);
    if (boost::ends_with(_mp_coll, irods::get_virtual_path_separator())) {
        for (size_t i = 1; i <= irods::get_virtual_path_separator().size(); i++) {
            coll_inp.collName[_mp_coll.size() - i] = '\0';
        }
    }

    int status = rsCollCreate(_comm, &coll_inp);
    if(status < 0) {
        std::string msg("failed to create [");
        msg += _mp_coll;
        msg += "]";
        THROW(status, msg);
    }

} // create_multipart_collection

void register_part_objects(
    rsComm_t*                        _comm,
    const irods::multipart_response& _resp) {

    dataObjInp_t obj_inp;
    openedDataObjInp_t opened_inp;

    for(auto& p : _resp.parts) {
        memset(&obj_inp, 0, sizeof(obj_inp));
        memset(&opened_inp, 0, sizeof(opened_inp));

        obj_inp.dataSize = p.part_size;
        rstrcpy(
            obj_inp.objPath,
            p.logical_path.c_str(),
            MAX_NAME_LEN);
        addKeyVal(
            &obj_inp.condInput,
            RESC_HIER_STR_KW,
            p.resource_hierarchy.c_str());
        addKeyVal(
            &obj_inp.condInput,
            FORCE_FLAG_KW, "");

        int inx = rsDataObjCreate(_comm, &obj_inp);
        if(inx < 0) {
            std::string msg("rsDataObjCreate failed for [");
            msg += p.logical_path;
            msg += "]";
            THROW(inx, msg);
        }

        opened_inp.l1descInx = inx;
        int status = rsDataObjClose(_comm, &opened_inp);
        if(status < 0) {
            std::string msg("rsDataObjClose failed for [");
            msg += p.logical_path;
            msg += "]";
            THROW(inx, msg);
        }

    } // for

} // register_part_objects

#endif

void multipart_executor_server(
        std::shared_ptr<irods::api_endpoint>  _ep_ptr ) {

    auto multipart_ep_ptr = std::dynamic_pointer_cast<irods::multipart_api_server_endpoint>(_ep_ptr);
#ifdef RODS_SERVER
    std::cout << "MULTIPART SERVER" << std::endl;

    try {
        rsComm_t* comm = multipart_ep_ptr->comm<rsComm_t*>();

        const auto& mp_req = multipart_ep_ptr->request();

        // =-=-=-=-=-=-=-
        //TODO: parameterize
        irods::message_broker bro{irods::zmq_type::RESPONSE, {.timeout = mp_req.timeout, .retries = mp_req.retries}};

        const int start_port = irods::get_server_property<const int>(
                                   irods::CFG_SERVER_PORT_RANGE_START_KW);
        const int  end_port = irods::get_server_property<const int>(
                                  irods::CFG_SERVER_PORT_RANGE_END_KW);
        const int port = bro.bind_to_port_in_range(start_port, end_port);
        multipart_ep_ptr->port(port);

        const size_t block_size = 4 * 1024 * 1024;

        switch (mp_req.operation) {
            case irods::PUT: {
                // wait for file size from client
                const auto file_size_string = bro.receive<std::string>();
                const off_t file_size = boost::lexical_cast<off_t>(file_size_string);

                const auto mp_coll = make_multipart_collection_path(
                                        mp_req.data_object_path);

                // do we need to restart? - if so find new offsets ( existing sizes ),
                // number of existing parts, logical paths, physical paths, hosts, hiers, etc
                const bool restart = query_for_restart(comm, mp_coll, multipart_ep_ptr->response());
                //const bool restart = false;

                if(!restart) {
                    rodsLog(LOG_NOTICE, "Starting fresh...");
                    const size_t num_parts = resolve_number_of_parts(mp_req.requested_number_of_parts, file_size, block_size);
                    multipart_ep_ptr->response().parts.resize(num_parts);

                    resolve_part_object_paths(mp_coll, multipart_ep_ptr->response());
                    resolve_part_hierarchies_for_put(comm, true, mp_req.resource, multipart_ep_ptr->response());
                    resolve_part_physical_paths(multipart_ep_ptr->response());

                    create_multipart_collection(comm, mp_coll);
                    register_part_objects(comm, multipart_ep_ptr->response());
                }

                resolve_part_sizes(file_size, block_size, multipart_ep_ptr->response().parts);
                resolve_part_hostnames(multipart_ep_ptr->response());

                break;
            }
            case irods::GET: {
                //send file size to client
                dataObjInp_t o_inp{};
                rstrcpy(o_inp.objPath, mp_req.data_object_path.c_str(), sizeof(o_inp.objPath));
                addKeyVal( &o_inp.condInput, SEL_OBJ_TYPE_KW, "dataObj" );

                rodsObjStat_t* stbuf{};
                int status = dataObjStat( comm, &o_inp, &stbuf );
                if ( status < 0 ) {
                    THROW(status, "failed in dataObjStat");
                }
                const off_t file_size = stbuf->objSize;

                const size_t num_parts = resolve_number_of_parts(mp_req.requested_number_of_parts, file_size, block_size);
                multipart_ep_ptr->response().parts.resize(num_parts);

                for (auto& p : multipart_ep_ptr->response().parts) {
                    p.logical_path = mp_req.data_object_path;
                }

                resolve_part_hierarchies_for_get(comm, true, mp_req.data_object_path, mp_req.resource, multipart_ep_ptr->response());
                resolve_part_physical_paths(multipart_ep_ptr->response());

                resolve_part_sizes(file_size, block_size, multipart_ep_ptr->response().parts);
                resolve_part_hostnames(multipart_ep_ptr->response());

                const auto rcv_msg = bro.receive();
                break;
            }
        }

        irods::server_transport_plugin_context transport_context{};
        transport_context.number_of_threads = resolve_number_of_threads(
            mp_req.requested_number_of_threads, multipart_ep_ptr->response().parts.size());

        transport_context.data_object_path = multipart_ep_ptr->request().data_object_path;
        transport_context.host_name = multipart_ep_ptr->response().parts.begin()->host_name;
        transport_context.operation = multipart_ep_ptr->request().operation;
        //TODO: remove parts from server_transport_plugin_context when multipart has actually been separated from unipart
        transport_context.parts = multipart_ep_ptr->response().parts;
        transport_context.timeout = multipart_ep_ptr->request().timeout;
        transport_context.retries = multipart_ep_ptr->request().retries;

        // =-=-=-=-=-=-=-
        // load and start server-side transport plugin
        auto xport_ep_ptr = irods::create_command_object(multipart_ep_ptr->request().transport_mechanism, irods::API_EP_SERVER);
        xport_ep_ptr->initialize_from_context(irods::convert_to_bytes(transport_context));

        auto xport_zmq_ctx = std::make_shared<zmq::context_t>(1);
        irods::message_broker cmd_skt(irods::zmq_type::REQUEST, {}, xport_zmq_ctx);
        cmd_skt.bind("inproc://server_comms");

        irods::api_v5_to_v5_call_endpoint(
            comm,
            xport_ep_ptr,
            xport_zmq_ctx);

        // =-=-=-=-=-=-=-
        // wait for port list
        cmd_skt.send(REQ_MSG);
        auto open_socket_list = cmd_skt.receive<irods::socket_address_list>();
        multipart_ep_ptr->response().port_list = open_socket_list.sockets;

        // respond to request from mp client exec
        bro.send(multipart_ep_ptr->response());

        // TODO: while message loop here
        const auto quit_rcv_msg = bro.receive();
        bro.send(ACK_MSG);

        xport_ep_ptr->wait();
        // =-=-=-=-=-=-=-
        // wait for finalize message from client

        // =-=-=-=-=-=-=-
        // apply metadata

        // =-=-=-=-=-=-=-
        // apply acls
    }
    catch(const boost::bad_any_cast& _e) {
        // end of protocol
        irods::log(LOG_ERROR, _e.what());
        multipart_ep_ptr->done(true);
        //TODO: notify client of failure
        THROW(INVALID_ANY_CAST, _e.what());
    }
    catch(const irods::exception& _e) {
        std::cout << "EXCEPTION: " << _e.what() << std::endl;
        throw;
    }
#endif

    std::cout << "EXIT" << std::endl;
    multipart_ep_ptr->done(true);

} // multipart_executor_server

void multipart_executor_server_to_server(
        std::shared_ptr<irods::api_endpoint>  _ep_ptr ) {
#ifdef RODS_SERVER
#endif
} // multipart_executor_server_to_server


//multipart api endpoint implementation

void irods::multipart_api_endpoint::capture_executors(
        irods::api_endpoint::thread_executor& _cli,
        irods::api_endpoint::thread_executor& _svr,
        irods::api_endpoint::thread_executor& _svr_to_svr) {
    _cli = multipart_executor_client;
    _svr = multipart_executor_server;
    _svr_to_svr = multipart_executor_server_to_server;
}

const std::set<std::string>& irods::multipart_api_client_endpoint::provides() const {
    static const std::set<std::string> provided_set{PUT_KW, GET_KW};
    return provided_set;
}

const std::tuple<std::string, po::options_description, po::positional_options_description>& irods::multipart_api_client_endpoint::get_program_options_and_usage(
        const std::string& _subcommand) const {
    static const std::map<std::string, std::tuple<std::string, po::options_description, po::positional_options_description>> options_and_usage_map{
        {PUT_KW, {
                "[OPTION]... source_physical_path destination_logical_path",
                []() {
                        po::options_description desc{"iRODS put"};
                        desc.add_options()
                            ("source_physical_path", po::value<std::string>(), "The path of the file or directory to be put into iRODS")
                            ("destination_logical_path", po::value<std::string>(), "The target path of the data object or collection in iRODS")
                            ("resource", po::value<std::string>(), "The resource in which to put the data object")
                            ("recursive", "Use this option to put a directory and all of its contents in iRODS, preserving the directory structure")
                            ("parts", po::value<int>(), "Number of parts to split the file into")
                            ("threads", po::value<int>(), "Number of threads to use")
                            ("timeout", po::value<int>(), "Timeout for the connection (in milliseconds)")
                            ("retries", po::value<int>(), "Number of times to retry connection before aborting in case of EAGAIN")
                            ;
                        return desc;
                    }(),
                []() {
                    po::positional_options_description positional_desc{};
                    positional_desc.add("source_physical_path", 1);
                    positional_desc.add("destination_logical_path", 1);
                    return positional_desc;
                }()
            }
        },
        {GET_KW, {
                "[OPTION]... source_logical_path destination_physical_path",
                []() {
                        po::options_description desc{"iRODS get"};
                        desc.add_options()
                            ("source_logical_path", po::value<std::string>(), "The path of the data object or collection to be retrieved from iRODS")
                            ("destination_physical_path", po::value<std::string>(), "The destination file or directory")
                            ("resource", po::value<std::string>(), "The resource from which to get the data object")
                            ("recursive", "Use this option to retrieve a collection and all of its contents from iRODS, preserving the directory structure")
                            ("parts", po::value<int>(), "Number of parts to split the file into")
                            ("threads", po::value<int>(), "Number of threads to use")
                            ("timeout", po::value<int>(), "Timeout for the connection (in milliseconds)")
                            ("retries", po::value<int>(), "Number of times to retry connection before aborting in case of EAGAIN")
                            ;
                        return desc;
                    }(),
                []() {
                    po::positional_options_description positional_desc{};
                    positional_desc.add("source_logical_path", 1);
                    positional_desc.add("destination_physical_path", 1);
                    return positional_desc;
                }()
            }
        }
    };
    return options_and_usage_map.at(_subcommand);
}

void irods::multipart_api_client_endpoint::initialize_from_command(
    const std::string&              _subcommand,
    const std::vector<std::string>& _args) {

    auto& program_options_and_usage = get_program_options_and_usage(_subcommand);
    po::variables_map vm;
    po::store(po::command_line_parser(_args).
            options(std::get<po::options_description>(program_options_and_usage)).
            positional(std::get<po::positional_options_description>(program_options_and_usage)).
            run(), vm);
    po::notify(vm);

    if (_subcommand == PUT_KW) {
        request().operation = irods::PUT;
        request().transport_mechanism = "api_plugin_unipart";
        context().local_filepath = vm["source_physical_path"].as<std::string>();
        request().data_object_path = construct_data_object_path(context().local_filepath, vm["destination_logical_path"].as<std::string>());
        //TODO: make this actually use the environment
        request().resource = vm.count("resource") ? vm["resource"].as<std::string>() : "demoResc";
        request().requested_number_of_parts = vm.count("parts") ? vm["parts"].as<int>() : 2;
        request().requested_number_of_threads = vm.count("threads") ? vm["threads"].as<int>() : 2;
        request().timeout = vm.count("timeout") ? vm["timeout"].as<int>() : -1;
        request().retries = vm.count("retries") ? vm["retries"].as<int>() : 1000;
    } else if (_subcommand == GET_KW) {
        request().operation = irods::GET;
        request().transport_mechanism = "api_plugin_unipart";
        context().local_filepath = vm["destination_physical_path"].as<std::string>();
        request().data_object_path = vm["source_logical_path"].as<std::string>();
        //TODO: make this actually use the environment
        request().resource = vm.count("resource") ? vm["resource"].as<std::string>() : "demoResc";
        request().requested_number_of_parts = vm.count("parts") ? vm["parts"].as<int>() : 2;
        request().requested_number_of_threads = vm.count("threads") ? vm["threads"].as<int>() : 2;
        request().timeout = vm.count("timeout") ? vm["timeout"].as<int>() : -1;
        request().retries = vm.count("retries") ? vm["retries"].as<int>() : 1000;
    } else {
        THROW(SYS_NOT_SUPPORTED, boost::format("Unsupported command: %s") % _subcommand);
    }
}

int irods::multipart_api_endpoint::status(rError_t* _err) const {
    if(status_ < 0) {
        addRErrorMsg(
            _err,
            status_,
            error_message_.str().c_str());
    }
    return status_;
}

extern "C" {
    irods::api_endpoint* plugin_factory(
            const std::string&,     //_inst_name
            const irods::connection_t& _connection_type ) { // _context
        switch(_connection_type) {
            case irods::API_EP_CLIENT:
                return new irods::multipart_api_client_endpoint(_connection_type);
            case irods::API_EP_SERVER:
            case irods::API_EP_SERVER_TO_SERVER:
                return new irods::multipart_api_server_endpoint(_connection_type);
        }
    }
};

