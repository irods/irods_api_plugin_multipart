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

#include "irods_stacktrace.hpp"
#include "irods_server_api_call.hpp"
#include "irods_re_serialization.hpp"
#include "irods_api_envelope.hpp"
#include "irods_api_endpoint.hpp"
#include "irods_virtual_path.hpp"
#include "irods_resource_manager.hpp"
#include "irods_resource_redirect.hpp"
#include "irods_hierarchy_parser.hpp"

#include "irods_multipart_request.hpp"
#include "irods_multipart_response.hpp"

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

    std::cout << __FUNCTION__ << ":" << __LINE__ << std::endl;
    std::cout << " number of threads: " << _resp.number_of_threads<< std::endl;
    std::cout << " number of threads: " << _resp.number_of_threads<< std::endl;
    for(auto p : _resp.port_list) {
        std::cout << "port: " << p << std::endl;
    }

    for(auto p : _resp.parts) {
        std::cout << "host_name: " << p.host_name << std::endl;
        std::cout << "file_size: " << p.file_size << std::endl;
        std::cout << "restart_offset: " << p.restart_offset << std::endl;
        std::cout << "original_offset: " << p.original_offset << std::endl;
        std::cout << "logical_path: " << p.logical_path << std::endl;
        std::cout << "resource_hierarchy: " << p.resource_hierarchy << std::endl;
    }
}


void multipart_executor_client(
        std::shared_ptr<irods::api_endpoint>  _endpoint ) {
    namespace bfs = boost::filesystem;
    typedef irods::message_broker::data_type data_t;


    try {
        rcComm_t* comm = nullptr;
        _endpoint->comm<rcComm_t*>(comm);

        irods::message_broker client_cmd_skt("ZMQ_REP", _endpoint->ctrl_ctx());
        client_cmd_skt.connect("inproc://client_comms");

        // =-=-=-=-=-=-=-
        //TODO: parameterize
        irods::message_broker bro("ZMQ_REQ");

        int port = _endpoint->port();
        std::stringstream conn_sstr;
        conn_sstr << "tcp://localhost:";
        conn_sstr << port;
        bro.connect(conn_sstr.str());

        // =-=-=-=-=-=-=-
        // fetch the payload to extract the request string
        irods::multipart_request mp_req;
        _endpoint->payload<irods::multipart_request>(mp_req);

        // =-=-=-=-=-=-=-
        // stat physical path
        bfs::path p(mp_req.file_path);

        if(!bfs::exists(p)) {
#if 0 // TODO - FIXME
            bro.send(QUIT_MSG);
            data_t rcv_msg;
            bro.receive(rcv_msg);
#endif
            _endpoint->done(true);
            return;
        }

        uintmax_t fsz = bfs::file_size(p);

        // =-=-=-=-=-=-=-
        // send file size to server
        // TODO: can we do this without string conversion?
        std::stringstream fsz_sstr; fsz_sstr << fsz;
        std::string fsz_str = fsz_sstr.str();

        data_t snd_msg(fsz_str.size());
        snd_msg.assign(fsz_str.begin(), fsz_str.end());
        bro.send(snd_msg);

        data_t rcv_msg;
        bro.receive(rcv_msg);
        auto in = avro::memoryInputStream(
                      &rcv_msg[0],
                      rcv_msg.size());
        auto dec = avro::binaryDecoder();
        dec->init( *in );
        irods::multipart_response mp_resp;
        avro::decode( *dec, mp_resp );

        auto xport_ep_ptr = irods::create_command_object(mp_req.transport_mechanism, irods::API_EP_CLIENT);

        zmq::context_t xport_zmq_ctx(1);
        irods::message_broker xport_cmd_skt("ZMQ_REQ", &xport_zmq_ctx);
        xport_cmd_skt.bind("inproc://client_comms");

        irods::api_v5_to_v5_call_client<irods::multipart_response>(
            comm,
            xport_ep_ptr,
            &xport_zmq_ctx,
            mp_resp);

        // command and control message loop
        while(true) {
            data_t cli_msg;
            client_cmd_skt.receive(cli_msg, ZMQ_DONTWAIT);
            if(cli_msg.size()>0) {
                // forward message from client to transport control
                //xport_cmd_skt.send(cli_msg);
                //xport_cmd_skt.receive(rcv_msg); // TODO: process?

                if(QUIT_MSG == cli_msg) {
                    // forward quit from client to multipart instance
                    bro.send(cli_msg);
                    data_t rcv_msg;
                    bro.receive(rcv_msg);
                    if(ACK_MSG != rcv_msg) {
                        std::cerr << "client thread reported an error: " << rcv_msg << std::endl;
                        // TODO: process response?
                    }
                    client_cmd_skt.send(rcv_msg);
                    break;
                }

                client_cmd_skt.send(rcv_msg);
            }
        } // while

        xport_ep_ptr->wait();

        _endpoint->done(true);
    }
    catch(const zmq::error_t& _e) {
        std::cerr << _e.what() << std::endl;
        _endpoint->done(true);
        THROW(SYS_SOCK_CONNECT_ERR, _e.what());
    }
    catch(const boost::bad_any_cast& _e) {
        std::cerr << _e.what() << std::endl;
        _endpoint->done(true);
        THROW(INVALID_ANY_CAST, _e.what());
    }
    catch(const boost::bad_lexical_cast& _e) {
        std::cerr << _e.what() << std::endl;
        _endpoint->done(true);
        THROW(INVALID_LEXICAL_CAST, _e.what());
    }
} // multipart_executor_client


#ifdef RODS_SERVER

// =-=-=-=-=-=-=-
// TODO: externalize this to the rule engine
static std::string make_multipart_collection_name(
        const std::string& _physical_path,
        const std::string& _logical_path) {
    namespace bfs = boost::filesystem;

    const std::string vps = irods::get_virtual_path_separator();
    std::string coll_path = _logical_path.substr(0, _logical_path.rfind(vps));

    // =-=-=-=-=-=-=-
    // build logical collection path for query
    return coll_path + vps + ".irods_" + bfs::path{_physical_path}.filename().string() + "_multipart" + vps;

} // make_multipart_collection_name

static std::string make_multipart_logical_path(
        const std::string& _phy_path,
        const std::string& _data_object_path) {
    namespace bfs = boost::filesystem;

    return boost::ends_with(_data_object_path, irods::get_virtual_path_separator()) ?
        _data_object_path + bfs::path{_phy_path}.filename().string() :
        _data_object_path;

} // make_multipart_logical_path

static bool query_for_restart(
    rsComm_t*                  _comm,
    const std::string&         _mp_coll,
    irods::multipart_response& _mp_resp ) {
    bool ret_val = false;

    // =-=-=-=-=-=-=-
    // determine if logical collection exists, get obj list
    std::string query{"select DATA_NAME, DATA_PATH, DATA_SIZE, DATA_RESC_ID where COLL_NAME like '" + _mp_coll + "%'"};

    genQueryInp_t gen_inp{};
    fillGenQueryInpFromStrCond( (char*)query.c_str(), &gen_inp );

    gen_inp.maxRows = MAX_SQL_ROWS;
    gen_inp.continueInx = 0;

    genQueryOut_t* gen_out = nullptr;

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
            pr.restart_offset = std::stol(&size->value[size->len * i]);

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


size_t resolve_number_of_parts(size_t _default) {
    // TODO: invoke policy here for num parts
    return _default;

} // resolve_number_of_parts

void resolve_part_object_paths(
    const std::string&         _dst_coll,
    const std::string&         _phy_path,
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

    size_t ctr = 0;
    bfs::path phy_path(_phy_path);
    for(auto& p : _resp.parts) {
        std::stringstream ss; ss << ctr;
        p.logical_path =
            _dst_coll +
            prefix +
            phy_path.filename().string() +
            ".irods_part_" +
            ss.str();
        ++ctr;
    }
} // resolve_part_object_paths

void resolve_part_sizes(
    uintmax_t                  _src_size,
    irods::multipart_response& _resp) {
    if(_resp.parts.empty()) {
        THROW(SYS_INVALID_INPUT_PARAM, "empty parts");
    }

    // compute evenly distributed size
    uintmax_t even_sz = _src_size / _resp.parts.size();

    // compute total fractional portion
    uintmax_t frac_sz = _src_size % _resp.parts.size();

    uintmax_t offset = 0;
    for(auto& p : _resp.parts) {
        p.file_size       = even_sz;
        p.original_offset = offset;
        offset += even_sz;
    }


    // add fractional portion to last part
    _resp.parts.rbegin()->file_size += frac_sz;

} // resolve_part_sizes

void resolve_number_of_threads(
    const int  _req_num,
    int&       _res_num) {

    // TODO: call PEP to externalize decision making
    _res_num = _req_num;

} // resolve_number_of_threads

void resolve_part_hierarchies(
    rsComm_t*                  _comm,
    bool                       _single_server,
    const std::string&         _dst_resc,
    irods::multipart_response& _resp) {

    dataObjInp_t obj_inp;
    if(_single_server) {
        memset(&obj_inp, 0, sizeof(obj_inp));

        auto& p = _resp.parts[0];

        obj_inp.dataSize = p.file_size;
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

            obj_inp.dataSize = p.file_size;
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
} // resolve_part_hierarchies

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

        obj_inp.dataSize = p.file_size;
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
        std::shared_ptr<irods::api_endpoint>  _endpoint ) {
#ifdef RODS_SERVER
    typedef irods::message_broker::data_type data_t;
    std::cout << "MULTIPART SERVER" << std::endl;

    try {
        rsComm_t* comm = nullptr;
        _endpoint->comm<rsComm_t*>(comm);

        // =-=-=-=-=-=-=-
        //TODO: parameterize
        irods::message_broker bro("ZMQ_REP");

        const int start_port = irods::get_server_property<const int>(
                                   irods::CFG_SERVER_PORT_RANGE_START_KW);
        const int  end_port = irods::get_server_property<const int>(
                                  irods::CFG_SERVER_PORT_RANGE_END_KW);
        int port = bro.bind_to_port_in_range(start_port, end_port);
        _endpoint->port(port);

        // =-=-=-=-=-=-=-
        // wait for file size from client
        data_t rcv_data;
        bro.receive(rcv_data);
        std::string file_size_string;
        file_size_string.assign(rcv_data.begin(), rcv_data.end());
        uintmax_t file_size = boost::lexical_cast<uintmax_t>(file_size_string);

        // =-=-=-=-=-=-=-
        // fetch the payload to extract the response string
        irods::multipart_request mp_req;
        _endpoint->payload<irods::multipart_request>(mp_req);

        std::string mp_coll = make_multipart_collection_name(
                                  mp_req.file_path,
                                  mp_req.data_object_path);

        // =-=-=-=-=-=-=-
        // do we need to restart? - if so find new offsets ( existing sizes ),
        // number of existing parts, logical paths, physical paths, hosts, hiers, etc
        irods::multipart_response mp_resp;
        mp_resp.file_path = mp_req.file_path;
        mp_resp.data_object_path = make_multipart_logical_path(
                                               mp_req.file_path,
                                               mp_req.data_object_path);

        bool restart = query_for_restart(comm, mp_coll, mp_resp);

        if(restart) {
            resolve_part_sizes(file_size, mp_resp);
            resolve_part_hostnames(mp_resp);
            resolve_number_of_threads(
                mp_req.requested_number_of_threads,
                mp_resp.number_of_threads);

            // =-=-=-=-=-=-=-
            // print out mp_resp for debug
            for(auto i : mp_resp.parts) {
                std::cout << "  hn: " << i.host_name
                          << "  sz: " << i.file_size
                          << "  of: " << i.restart_offset
                          << "  of: " << i.original_offset
                          << "  dp: " << i.logical_path
                          << "  pp: " << i.physical_path
                          << "  rh: " << i.resource_hierarchy
                          << std::endl;
            }
        }
        else {
            // determine number of parts
            size_t num_parts = resolve_number_of_parts(mp_req.requested_number_of_parts);
            mp_resp.parts.resize(num_parts);

            // determine obj paths
            resolve_part_object_paths(mp_coll, mp_req.file_path, mp_resp);

            // 3. resolve part sizes
            resolve_part_sizes(file_size, mp_resp);

            // resolve number of threads
            resolve_number_of_threads(
                mp_req.requested_number_of_threads,
                mp_resp.number_of_threads);

            // resolve hierarchy for all parts
            resolve_part_hierarchies(comm, true, mp_req.resource, mp_resp);

            // resolve part hosts
            resolve_part_hostnames(mp_resp);

            // resolve part physical paths
            resolve_part_physical_paths(mp_resp);

            // create multipart collection
            create_multipart_collection(comm, mp_coll);

            // bulk-reg/create part objects
            register_part_objects(comm, mp_resp);
        }

        // =-=-=-=-=-=-=-
        // load and start server-side transport plugin
        auto xport_ep_ptr = irods::create_command_object(mp_req.transport_mechanism, irods::API_EP_SERVER);

        zmq::context_t xport_zmq_ctx(1);
        irods::message_broker cmd_skt("ZMQ_REQ", &xport_zmq_ctx);
        cmd_skt.bind("inproc://server_comms");

        irods::api_v5_to_v5_call_server<irods::multipart_response>(
            comm,
            xport_ep_ptr,
            &xport_zmq_ctx,
            mp_resp);

        // =-=-=-=-=-=-=-
        // wait for response object from client
        cmd_skt.send(REQ_MSG);
        data_t rcv_msg;
        cmd_skt.receive(rcv_msg);

        // respond to request from mp client exec
        bro.send(rcv_msg);

        // TODO: while message loop here
        data_t quit_rcv_msg;
        bro.receive(quit_rcv_msg);
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
        _endpoint->done(true);
        //TODO: notify client of failure
        THROW(INVALID_ANY_CAST, _e.what());
    }
    catch(const irods::exception& _e) {
        std::cout << "EXCEPTION: " << _e.what() << std::endl;
        throw;
    }
#endif

    std::cout << "EXIT" << std::endl;
    _endpoint->done(true);

} // multipart_executor_server

void multipart_executor_server_to_server(
        std::shared_ptr<irods::api_endpoint>  _endpoint ) {
#ifdef RODS_SERVER
#endif
} // multipart_executor_server_to_server


class multipart_api_endpoint : public irods::api_endpoint {
    public:
        const std::string PUT_KW{"put"};
        const std::string GET_KW{"get"};

        // =-=-=-=-=-=-=-
        // provide thread executors to the invoke() method
        void capture_executors(
                thread_executor& _cli,
                thread_executor& _svr,
                thread_executor& _svr_to_svr) {
            _cli        = multipart_executor_client;
            _svr        = multipart_executor_server;
            _svr_to_svr = multipart_executor_server_to_server;
        }

        multipart_api_endpoint(const irods::connection_t _connection_type) :
            irods::api_endpoint(_connection_type) {
                name_ = "api_plugin_multipart";
        }

        ~multipart_api_endpoint() {
        }

        std::set<std::string> provides() {
            return { PUT_KW, GET_KW };
        }

        const std::tuple<std::string, po::options_description, po::positional_options_description>& get_program_options_and_usage(const std::string& _subcommand) {
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
                                    ("recursive", "Use this option to retrieve a collection and all of its contents from iRODS, preserving the directory structure")
                                    ("parts", po::value<int>(), "Number of parts to split the file into")
                                    ("threads", po::value<int>(), "Number of threads to use")
                                    ;
                                return desc;
                            }(),
                        []() {
                            po::positional_options_description positional_desc{};
                            positional_desc.add("source_logical_path", 1);
                            positional_desc.add("destination_logical_path", 1);
                            return positional_desc;
                        }()
                    }
                }
            };
            return options_and_usage_map.at(_subcommand);
        }

        irods::multipart_request get_request_from_command_args(const std::string& _subcommand, const std::vector<std::string>& _args) {
            auto& program_options_and_usage = get_program_options_and_usage(_subcommand);
            po::variables_map vm;
            po::store(po::command_line_parser(_args).
                    options(std::get<po::options_description>(program_options_and_usage)).
                    positional(std::get<po::positional_options_description>(program_options_and_usage)).
                    run(), vm);
            po::notify(vm);

            if (_subcommand == PUT_KW) {
                irods::multipart_request req{};
                req.operation = irods::MULTIPART_PUT;
                req.transport_mechanism = "api_plugin_unipart";
                req.file_path = vm["source_physical_path"].as<std::string>();
                req.data_object_path = vm["destination_logical_path"].as<std::string>();
                //TODO: make this actually use the environment
                req.resource = vm.count("resource") ? vm["resource"].as<std::string>() : "demoResc";
                req.requested_number_of_parts = vm.count("parts") ? vm["parts"].as<int>() : 2;
                req.requested_number_of_threads = vm.count("threads") ? vm["threads"].as<int>() : 2;
                return req;
            } else if (_subcommand == GET_KW) {
                irods::multipart_request req{};
                req.operation = irods::MULTIPART_GET;
                req.transport_mechanism = "api_plugin_unipart";
                req.file_path = vm["destination_physical_path"].as<std::string>();
                req.data_object_path = vm["source_logical_path"].as<std::string>();
                //TODO: make this actually use the environment
                req.resource = vm.count("resource") ? vm["resource"].as<std::string>() : "";
                req.requested_number_of_parts = vm.count("parts") ? vm["parts"].as<int>() : 2;
                req.requested_number_of_threads = vm.count("threads") ? vm["threads"].as<int>() : 2;
                return req;
            }
            THROW(SYS_NOT_SUPPORTED, boost::format("Unsupported command: %s") % _subcommand);
        }

        // =-=-=-=-=-=-=-
        // used for client-side initialization
        void init_and_serialize_payload(
            const std::string&              _subcommand,
            const std::vector<std::string>& _args,
            std::vector<uint8_t>&           _out) {

            irods::multipart_request mp_req = get_request_from_command_args(_subcommand, _args);

            auto out = avro::memoryOutputStream();
            auto enc = avro::binaryEncoder();
            enc->init( *out );
            avro::encode( *enc, mp_req );
            auto data = avro::snapshot( *out );

            // copy for transmission to server
            _out = *data;

            // copy for client side use also
            payload_ = mp_req;
        }

        // =-=-=-=-=-=-=-
        // used for server-side initialization
        void decode_and_assign_payload(
            const std::vector<uint8_t>& _in) {
            auto in = avro::memoryInputStream(
                          &_in[0],
                          _in.size());
            auto dec = avro::binaryDecoder();
            dec->init( *in );
            irods::multipart_request mp_req;
            avro::decode( *dec, mp_req );
            payload_ = mp_req;
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
        const std::string&,     //_inst_name
        const irods::connection_t& _connection_type ) { // _context
            return new multipart_api_endpoint(_connection_type);
    }
};

