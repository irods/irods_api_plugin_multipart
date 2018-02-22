#include <memory>

#include "boost/program_options.hpp"

#include "irods_api_endpoint.hpp"
#include "irods_multipart_avro_types.hpp"
#include "irods_unipart_request.hpp"

namespace irods {
    class unipart_api_endpoint :
        public virtual api_endpoint,
        public virtual with_request<unipart_request>,
        public virtual without_response,
        public virtual with_cli_disabled,
        public virtual without_context_initialization {
            public:
                std::shared_ptr<message_broker> inherited_broker_;

                unipart_api_endpoint(
                        const connection_t _connection_type) :
                    api_endpoint{_connection_type} {
                        name_ = "api_plugin_unipart";
                    }

                virtual ~unipart_api_endpoint() {}

                void
                    capture_executors(
                            thread_executor& _cli,
                            thread_executor& _svr,
                            thread_executor& _svr_to_svr);

                // =-=-=-=-=-=-=-
                // provide an error code and string to the client
                int
                    status(
                            rError_t* _err) const;

            protected:
                std::stringstream error_message_;
        };

    class unipart_api_client_endpoint :
        public virtual unipart_api_endpoint,
        public virtual with_context<client_transport_plugin_context> {
            public:
                unipart_api_client_endpoint(
                        const connection_t _connection_type ) :
                    api_endpoint{_connection_type},
                    unipart_api_endpoint{_connection_type} {}

                void
                    initialize_from_context(
                            const std::vector<uint8_t>& _bytes);

        };
    class unipart_api_server_endpoint :
        public virtual unipart_api_endpoint,
        public virtual with_context<server_transport_plugin_context> {
            public:
                unipart_api_server_endpoint(
                        const connection_t _connection_type ) :
                    api_endpoint{_connection_type},
                    unipart_api_endpoint{_connection_type} {}

                void
                    initialize_from_context(
                            const std::vector<uint8_t>& _bytes);

        };
}
