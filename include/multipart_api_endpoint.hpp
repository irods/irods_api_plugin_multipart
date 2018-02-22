#include "boost/program_options.hpp"
#include "boost/variant.hpp"

#include "irods_api_endpoint.hpp"
#include "irods_multipart_avro_types.hpp"

namespace irods {
    class multipart_api_endpoint :
        public virtual api_endpoint,
        public virtual with_request<multipart_request>,
        public virtual with_response<multipart_response>,
        public virtual without_context_initialization {
            public:
                const std::string PUT_KW{"put"};
                const std::string GET_KW{"get"};

                multipart_api_endpoint(
                        const connection_t _connection_type) :
                    api_endpoint{_connection_type},
                    error_message_{} {
                        name_ = "api_plugin_multipart";
                    }

                virtual ~multipart_api_endpoint() {}

                // =-=-=-=-=-=-=-
                // provide thread executors to the invoke() method
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

    class multipart_api_client_endpoint :
        public virtual multipart_api_endpoint,
        public virtual with_context<client_multipart_plugin_context> {
            public:
                multipart_api_client_endpoint(
                        const connection_t _connection_type) :
                    api_endpoint{_connection_type},
                    multipart_api_endpoint{_connection_type} {}

                virtual ~multipart_api_client_endpoint() {}

                const std::set<std::string>&
                    provides() const;

                const std::tuple<std::string, boost::program_options::options_description, boost::program_options::positional_options_description>&
                    get_program_options_and_usage(
                            const std::string& _subcommand) const;

                void
                    initialize_from_command(
                            const std::string& _subcommand,
                            const std::vector<std::string>& _args);

        };

    class multipart_api_server_endpoint :
        public virtual multipart_api_endpoint,
        public virtual without_context,
        public virtual with_cli_disabled {
            public:
                multipart_api_server_endpoint(
                        const connection_t _connection_type) :
                    api_endpoint{_connection_type},
                    multipart_api_endpoint{_connection_type} {}

                virtual ~multipart_api_server_endpoint() {}
        };
}
