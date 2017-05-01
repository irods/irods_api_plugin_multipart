// =-=-=-=-=-=-=-
// irods includes
#include "apiHandler.hpp"
#include "rodsPackInstruct.h"
#include "objStat.h"
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
#include <boost/algorithm/string/predicate.hpp>

// =-=-=-=-=-=-=-
// stl includes
#include <sstream>
#include <string>
#include <iostream>

extern irods::resource_manager resc_mgr;
            
static const irods::message_broker::data_type REQ_MSG = {'R', 'E', 'Q'};
static const irods::message_broker::data_type ACK_MSG = {'A', 'C', 'K'};
static const irods::message_broker::data_type QUIT_MSG = {'q', 'u', 'i', 't'};

void print_mp_response(
        const irods::multipart_response& _resp) {

    std::cout << __FUNCTION__ << ":" << __LINE__ << std::endl;
    std::cout << " number of threads: " << _resp.number_of_threads<< std::endl;
    for(auto p : _resp.port_list) {
        std::cout << "port: " << p << std::endl;
    }

    for(auto p : _resp.parts) {
        std::cout << "host_name: " << p.host_name << std::endl;
        std::cout << "file_size: " << p.file_size << std::endl;
        std::cout << "restart_offset: " << p.restart_offset << std::endl;
        std::cout << "original_offset: " << p.original_offset << std::endl;
        std::cout << "destination_logical_path: " << p.destination_logical_path << std::endl;
        std::cout << "resource_hierarchy: " << p.resource_hierarchy << std::endl;
    }
}


void multipart_executor_client(
        irods::api_endpoint*  _endpoint ) {
    namespace bfs = boost::filesystem;
    typedef irods::message_broker::data_type data_t;

    zmq::socket_t client_cmd_skt(*_endpoint->ctrl_ctx(), ZMQ_REP);
    client_cmd_skt.connect("inproc://client_comms");

    try {
        rcComm_t* comm = nullptr;
        _endpoint->comm<rcComm_t*>(comm);

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
        bfs::path p(mp_req.source_physical_path);

        if(!bfs::exists(p)) {
            bro.send(QUIT_MSG);
            data_t rcv_msg;
            bro.receive(rcv_msg);
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

        zmq::context_t xport_zmq_ctx(1);
        irods::api_endpoint* xport_ep_ptr = nullptr;

        irods::message_broker xport_cmd_skt("ZMQ_REQ", &xport_zmq_ctx);
        xport_cmd_skt.bind("inproc://client_comms");

        irods::api_v5_to_v5_call_client<irods::multipart_response>(
            comm,
            mp_req.operation,
            xport_ep_ptr,
            &xport_zmq_ctx,
            mp_resp); 

        // send a request to the client side transport
        xport_cmd_skt.send(REQ_MSG);

        // wait for a status response
        data_t cmd_rcv_msg;
        xport_cmd_skt.receive(cmd_rcv_msg);

        std::string cmd_rcv_msg_str;
        cmd_rcv_msg_str.assign(cmd_rcv_msg.begin(), cmd_rcv_msg.end());

        while(true) {
            zmq::message_t rcv_msg;
            bool ret = client_cmd_skt.recv( &rcv_msg, ZMQ_DONTWAIT);
            if( ret || rcv_msg.size() > 0) {
 
                std::string in_str;
                in_str.assign(
                    (char*)rcv_msg.data(),
                    ((char*)rcv_msg.data())+rcv_msg.size());

                std::cout << "CMD RECV - [" << in_str << "]" << std::endl; 
                // =-=-=-=-=-=--=
                // process events from the client 
                if("quit" == in_str) {
                    #if 0
                    // notify server of the event
                    std::cout << "CMD sending quit" << std::endl;
                    data_t req_data;
                    req_data.assign(in_str.begin(), in_str.end()); 
                    bro.send( req_data );
                    
                    data_t resp_data;
                    bro.receive(resp_data);
                    #endif
                    zmq::message_t snd_msg(3);
                    memcpy(snd_msg.data(), "ACK", 3);
                    client_cmd_skt.send(snd_msg);
                    break;
                }

                zmq::message_t snd_msg(3);
                memcpy(snd_msg.data(), "ACK", 3);
                client_cmd_skt.send(snd_msg);

            } // if event

        } // while true
        
        xport_ep_ptr->wait();

        _endpoint->done(true);
    }
    catch(const zmq::error_t& _e) {
        std::cerr << _e.what() << std::endl;
        _endpoint->done(true);
    }
    catch(const boost::bad_any_cast& _e) {
        std::cerr << _e.what() << std::endl;
        _endpoint->done(true);
    }
    catch(const boost::bad_lexical_cast& _e) {
        std::cerr << _e.what() << std::endl;
        _endpoint->done(true);
    }

} // multipart_executor_client


#ifdef RODS_SERVER

// =-=-=-=-=-=-=-
// TODO: externalize this to the rule engine
static std::string make_multipart_collection_name(
        const std::string& _phy_path,
        const std::string& _coll_path) {
    namespace bfs = boost::filesystem;

    std::string prefix; 
    const std::string vps = irods::get_virtual_path_separator();
    if( !boost::ends_with(_coll_path, vps)) {
        prefix += vps;
    }
    prefix += ".irods_";

    // =-=-=-=-=-=-=-
    // build logical collection path for query
    bfs::path phy_path(_phy_path);
    return _coll_path + prefix + phy_path.filename().string() + "_multipart";

} // make_multipart_collection_name

static bool query_for_restart(
    rsComm_t*                  _comm,
    const std::string&         _mp_coll,
    irods::multipart_response& _mp_resp ) {
    bool ret_val = false;

    // =-=-=-=-=-=-=-
    // determine if logical collection exists, get obj list
    std::string query("select DATA_NAME, DATA_SIZE, DATA_RESC_ID where COLL_NAME like '");
    query += _mp_coll + "%'";

    genQueryInp_t gen_inp;
    memset(&gen_inp, 0, sizeof(gen_inp));
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
        sqlResult_t* size    = getSqlResultByInx( gen_out, COL_DATA_SIZE );
        sqlResult_t* resc_id = getSqlResultByInx( gen_out, COL_D_RESC_ID );
        for(auto i = 0; i < gen_out->rowCnt; ++i) {
            irods::part_request pr;
            pr.destination_logical_path = _mp_coll + &name->value[name->len * i];
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
        p.destination_logical_path =
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
    double even_sz = (double)_src_size/(double)_resp.parts.size();

    // remove fractional portion
    double trunc_sz = std::trunc(even_sz);

    for(auto& p : _resp.parts) {
        p.file_size = trunc_sz;
    }

    // compute total fractional portion
    double frac_sz = (even_sz - trunc_sz)*_resp.parts.size();

    // add fractional portion to first part
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
    irods::multipart_response& _resp) {

    dataObjInp_t obj_inp;
    if(_single_server) {
        memset(&obj_inp, 0, sizeof(obj_inp));

        auto& p = _resp.parts[0];

        obj_inp.dataSize = p.file_size;
        rstrcpy(
                obj_inp.objPath,
                p.destination_logical_path.c_str(),
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
                p.destination_logical_path.c_str(),
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

void resolve_hostnames(
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
} // resolve_hostnames

void create_multipart_collection(
    rsComm_t*          _comm,
    const std::string& _mp_coll) {

    collInp_t coll_inp;
    memset(&coll_inp, 0, sizeof(coll_inp));

    addKeyVal(
        &coll_inp.condInput,
        RECURSIVE_OPR__KW, "");
    rstrcpy(
        coll_inp.collName,
        _mp_coll.c_str(),
        MAX_NAME_LEN);
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
            p.destination_logical_path.c_str(),
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
            msg += p.destination_logical_path;
            msg += "]";
            THROW(inx, msg);
        }

        opened_inp.l1descInx = inx;
        int status = rsDataObjClose(_comm, &opened_inp);
        if(status < 0) {
            std::string msg("rsDataObjClose failed for [");
            msg += p.destination_logical_path;
            msg += "]";
            THROW(inx, msg);
        }

    } // for

} // register_part_objects

#endif

void multipart_executor_server(
        irods::api_endpoint*  _endpoint ) {
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
        std::cout << "Got file size: " << file_size_string << std::endl;
        uintmax_t file_size = boost::lexical_cast<uintmax_t>(file_size_string);

        // =-=-=-=-=-=-=-
        // fetch the payload to extract the response string
        irods::multipart_request mp_req;
        _endpoint->payload<irods::multipart_request>(mp_req);

        // =-=-=-=-=-=-=-
        // start with printing the values
        std::cout << "XXXX - operation: " << mp_req.operation << std::endl;
        std::cout << "XXXX - source_physical_path: " << mp_req.source_physical_path << std::endl;
        std::cout << "XXXX - destination_collection: " << mp_req.destination_collection << std::endl;
        std::cout << "XXXX - destination_resource: " << mp_req.destination_resource << std::endl;
        std::cout << "XXXX - number_of_parts: " << mp_req.requested_number_of_parts << std::endl;
        std::cout << "XXXX - number_of_threads: " << mp_req.requested_number_of_threads << std::endl;

        std::string mp_coll = make_multipart_collection_name(
                                  mp_req.source_physical_path,
                                  mp_req.destination_collection);

        // =-=-=-=-=-=-=-
        // do we need to restart? - if so find new offsets ( existing sizes ),
        // number of existing parts, logical paths, physical paths, hosts, hiers, etc
        irods::multipart_response mp_resp;
        mp_resp.source_physical_path = mp_req.source_physical_path;

        bool restart = query_for_restart(comm, mp_coll, mp_resp);

        if(restart) {
            resolve_part_sizes(file_size, mp_resp); 
            resolve_hostnames(mp_resp);
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
                          << "  dp: " << i.destination_logical_path
                          << "  rh: " << i.resource_hierarchy
                          << std::endl;
            }
        }
        else {
            // =-=-=-=-=-=-=-
            // 1. determine number of parts
            size_t num_parts = resolve_number_of_parts(mp_req.requested_number_of_parts);
            mp_resp.parts.resize(num_parts);

            // =-=-=-=-=-=-=-
            // 2. determine obj paths
            resolve_part_object_paths(mp_coll, mp_req.source_physical_path, mp_resp); 

            // =-=-=-=-=-=-=-
            // 3. resolve part sizes
            resolve_part_sizes(file_size, mp_resp); 
            
            // =-=-=-=-=-=-=-
            // 4. resolve number of threads
            resolve_number_of_threads(
                mp_req.requested_number_of_threads,
                mp_resp.number_of_threads);

            // =-=-=-=-=-=-=-
            // X. resolve hierarchy for all parts
            resolve_part_hierarchies(comm, true, mp_resp);

            // =-=-=-=-=-=-=-
            // X. resolve part hosts
            resolve_hostnames(mp_resp);

print_mp_response(mp_resp);

            // =-=-=-=-=-=-=-
            // X. create multipart collection
            create_multipart_collection(comm, mp_coll);

            // =-=-=-=-=-=-=-
            // X. bulk-reg/create part objects
            register_part_objects(comm, mp_resp);

            // =-=-=-=-=-=-=-
            // X. find open ports
        }

        // =-=-=-=-=-=-=-
        // load and start server-side transport plugin
        zmq::context_t xport_zmq_ctx(1);
        irods::api_endpoint* xport_ep_ptr = nullptr;

        zmq::socket_t cmd_skt(xport_zmq_ctx, ZMQ_REQ);
        cmd_skt.bind("inproc://server_comms");
        
        irods::api_v5_to_v5_call_server<irods::multipart_response>(
            comm,
            mp_req.operation,
            xport_ep_ptr,
            &xport_zmq_ctx,
            mp_resp); 

        // =-=-=-=-=-=-=-
        // wait for finalize message from client
        zmq::message_t snd_msg(3);
        memcpy(snd_msg.data(), "REQ", 3);
        cmd_skt.send(snd_msg);

        zmq::message_t rcv_msg;
        while(true) {         
            cmd_skt.recv( &rcv_msg );
            if(rcv_msg.size()<= 0) {
                //TODO: need backoff
                continue;
            }
            break;
        }

        bro.send(rcv_msg);

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
        return;
    }
    catch(const irods::exception& _e) {
        std::cout << "EXCEPTION: " << _e.what() << std::endl;
    }
#endif

    std::cout << "EXIT" << std::endl;
    _endpoint->done(true);

} // multipart_executor_server

void multipart_executor_server_to_server(
        irods::api_endpoint*  _endpoint ) {
#ifdef RODS_SERVER
#endif
} // multipart_executor_server_to_server


class multipart_api_endpoint : public irods::api_endpoint {
    public:
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

        multipart_api_endpoint(const std::string& _ctx) :
            irods::api_endpoint(_ctx) {
        }

        ~multipart_api_endpoint() {
        }

        // =-=-=-=-=-=-=-
        // used for client-side initialization
        void init_and_serialize_payload(
            const std::vector<std::string>& _args,
            std::vector<uint8_t>&           _out) {
            if(_args.size()<8) {
                std::cerr << "api_plugin_multipart: operation physical_path "
                          << "logical_path destination_resource number_of_parts" 
                          << std::endl;
                // TODO: throw irods exception here
                return;
            }

            for( auto i : _args ) {
                std::cout << "arg["<<i<<"]" << std::endl;
            }

            //TODO: hit this with boost::program_options
            irods::multipart_request mp_req;
            mp_req.operation                 = _args[2];
            mp_req.source_physical_path      = _args[3];
            mp_req.destination_collection    = _args[4];
            mp_req.destination_resource      = _args[5];
            mp_req.requested_number_of_parts   = atoi(_args[6].c_str());
            mp_req.requested_number_of_threads = atoi(_args[7].c_str());

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
        const std::string& _context ) { // _context
            return new multipart_api_endpoint(_context);
    }
};

