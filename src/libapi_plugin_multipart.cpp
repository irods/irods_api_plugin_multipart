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

#include "irods_multipart_request.hpp"

#include "boost/lexical_cast.hpp"

// =-=-=-=-=-=-=-
// stl includes
#include <sstream>
#include <string>
#include <iostream>

void multipart_executor_client(
        irods::api_endpoint*  _endpoint ) {
    return;
} // multipart_executor

void multipart_executor_server(
        irods::api_endpoint*  _endpoint ) {
    return;
} // multipart_executor

void multipart_executor_server_to_server(
        irods::api_endpoint*  _endpoint ) {
    return;
} // multipart_executor


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
            int                   _argc,
            char*                 _argv[],
            std::vector<uint8_t>& _out) {
            if(_argc<7) {
                std::cerr << "api_plugin_multipart: operation physical_path "
                          << "logical_path destination_resource number_of_parts" 
                          << std::endl;
                // TODO: throw irods exception here
                return;
            }

            for( auto i=0; i<_argc; ++i) {
                std::cout << "arg["<<i<<"] = " << _argv[i] << std::endl;
            }

            //TODO: hit this with boost::program_options
            irods::multipart_request mp_req;
            mp_req.operation = _argv[2];
            mp_req.physical_path = _argv[3];
            mp_req.logical_path = _argv[4];
            mp_req.destination_resource = _argv[5];
            mp_req.number_of_parts = atoi(_argv[6]);

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
        // function which captures any final output to respond back
        // to the client using the legacy protocol
        void finalize(std::vector<uint8_t>*& _out) {
            char msg[] = { "this is the OUTPUT message from FINALIZE" };
            _out = new std::vector<uint8_t>();

            _out->resize(sizeof(msg));
            memcpy(_out->data(), msg, sizeof(msg));
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

