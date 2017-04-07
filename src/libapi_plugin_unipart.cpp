// =-=-=-=-=-=-=-
// irods includes
#include "apiHandler.hpp"
#include "rodsPackInstruct.h"
#include "objStat.h"

#include "irods_stacktrace.hpp"
#include "irods_server_api_call.hpp"
#include "irods_re_serialization.hpp"
#include "irods_api_envelope.hpp"
#include "irods_api_endpoint.hpp"
#include "irods_virtual_path.hpp"

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

void unipart_executor_client(
        irods::api_endpoint*  _endpoint ) {
    //typedef irods::message_broker::data_type data_t;
    std::cout << "MULTIPART CLIENT" << std::endl;

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
    irods::unipart_request up_req;
    try {
        _endpoint->payload<irods::unipart_request>(up_req);
    }
    catch(const boost::bad_any_cast& _e) {
        // end of protocol
        std::cerr << _e.what() << std::endl;
        _endpoint->done(true);
        //TODO: notify server of failure
        return;
    }

    // =-=-=-=-=-=-=-
    // request port range and restart?

    return;
} // unipart_executor

static std::string make_unipart_collection_name(
    const std::string& _phy_path,
    const std::string& _coll_path) {
    namespace bfs = boost::filesystem;
    const std::string vps = irods::get_virtual_path_separator();

    // =-=-=-=-=-=-=-
    // build logical collection path for query
    bfs::path phy_path(_phy_path);
    return _coll_path + vps + ".irods_" + phy_path.filename().string() + "_unipart";

} // make_unipart_collection_name

static void query_for_restart(
    irods::unipart_request& _mp_req) {
    std::string mp_coll = make_unipart_collection_name(
                              _mp_req.source_physical_path,
                              _mp_req.destination_logical_path);
    std::cout << "XXXX - " << __FUNCTION__ << "mp_coll: " << mp_coll << std::endl;

    // =-=-=-=-=-=-=-
    // determine if logical collection exists, get obj list

    // =-=-=-=-=-=-=-
    // if obj list exists, build restart part array objs


    // =-=-=-=-=-=-=-
    // 

    // =-=-=-=-=-=-=-
    // 

    // =-=-=-=-=-=-=-
    // 

    // =-=-=-=-=-=-=-
    // 
} // query_for_restart

void unipart_executor_server(
        irods::api_endpoint*  _endpoint ) {
    //typedef irods::message_broker::data_type data_t;
    std::cout << "MULTIPART SERVER" << std::endl;

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
    // fetch the payload to extract the response string
    irods::unipart_request up_req;
    try {
        _endpoint->payload<irods::unipart_request>(up_req);
    }
    catch(const boost::bad_any_cast& _e) {
        // end of protocol
        irods::log(LOG_ERROR, _e.what());
        _endpoint->done(true);
        //TODO: notify client of failure
        return;
    }

    // =-=-=-=-=-=-=-
    // start with printing the values

    // =-=-=-=-=-=-=-
    // do we need to restart? - if so find new offsets ( existing sizes ),
    // number of existing parts, logical paths, physical paths, hosts, hiers, etc
    query_for_restart(up_req);

    // =-=-=-=-=-=-=-
    // determine number of parts

    // =-=-=-=-=-=-=-
    // determine ports for each xfer thread

    // =-=-=-=-=-=-=-
    // wait for request for ports/restart

    // =-=-=-=-=-=-=-
    // respond with configuration
    // NOTE: use pub-sub and random topic strings for security!?

    // =-=-=-=-=-=-=-
    // spin up xfer threads

    // =-=-=-=-=-=-=-
    // wait for GO

    // =-=-=-=-=-=-=-
    // control loop for pause/cancel/restart/etc

    return;
} // unipart_executor

void unipart_executor_server_to_server(
        irods::api_endpoint*  _endpoint ) {
    return;
} // unipart_executor


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
            // =-=-=-=-=-=-=-
            // check number of paramters 
            if(_args.size() < 7) {
                std::cerr << "api_plugin_unipart: operation physical_path "
                          << "logical_path destination_resource number_of_parts" 
                          << std::endl;
                // TODO: throw irods exception here
                return;
            }

            // =-=-=-=-=-=-=-
            // XXXX - debug output
            for( auto i : _args ) {
                std::cout << "arg["<<i<<"]" << std::endl;
            }

            // =-=-=-=-=-=-=-
            //TODO: hit this with boost::program_options
            irods::unipart_request up_req;
            up_req.operation                         = _args[2];
            up_req.source_physical_path              = _args[3];
            up_req.source_file_size                  = std::stol(_args[4]);
            up_req.destination_logical_path          = _args[5];
            up_req.destination_resource_or_hierarchy = _args[6];

            auto out = avro::memoryOutputStream();
            auto enc = avro::binaryEncoder();
            enc->init( *out );
            avro::encode( *enc, up_req );
            auto data = avro::snapshot( *out );

            // copy for transmission to server
            _out = *data;

            // copy for client side use also
            payload_ = up_req;
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
            irods::unipart_request up_req;
            avro::decode( *dec, up_req );
            payload_ = up_req;
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
            return new unipart_api_endpoint(_context);
    }
};

