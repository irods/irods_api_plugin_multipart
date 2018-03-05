#ifndef DATA_TRANSFER_HPP__
#define DATA_TRANSFER_HPP__

#include <cstring>
#include <iostream>

#include "boost/make_shared.hpp"

#include "rcConnect.h"
#include "fileDriver.hpp"
#include "irods_resource_types.hpp"
#include "irods_file_object.hpp"
#include "irods_exception.hpp"
#include "irods_log.hpp"
#include "irods_message_broker.hpp"

#include "irods_memory_mapped_file.hpp"
#include "irods_unipart_request.hpp"


static const irods::message_broker::data_type ACK_MSG = {'A', 'C', 'K'};
static const irods::message_broker::data_type REQ_MSG = {'R', 'E', 'Q'};
static const irods::message_broker::data_type QUIT_MSG = {'q', 'u', 'i', 't'};
static const irods::message_broker::data_type FINALIZE_MSG = {'F', 'I', 'N', 'A', 'L', 'I', 'Z', 'E'};
static const irods::message_broker::data_type ERROR_MSG = {'e', 'r', 'r', 'o', 'r'};
static const irods::message_broker::data_type PROG_MSG = {'p', 'r', 'o', 'g', 'r', 'e', 's', 's'};

namespace irods {
    class multipart_method {
        public:
        virtual size_t client_transfer(
                message_broker& _bro,
                std::shared_ptr<memory_mapped_file> _mmapped_file,
                const unipart_request& _uni_req,
                const size_t _bytes_remaining,
                const size_t _block_size) = 0;
        virtual void client_cleanup(
                message_broker& _bro) = 0;
        virtual size_t server_transfer(
                rsComm_t* _comm,
                message_broker& _bro,
                const unipart_request& _uni_req,
                const size_t _bytes_remaining,
                const size_t _block_size) = 0;
        virtual void server_cleanup(
                rsComm_t*          _comm,
                resource_ptr _resc) = 0;
        //virtual size_t server_to_server_transfer() = delete;
    };

    class put : public multipart_method {
        private:
            off_t offset_;
            file_object_ptr file_obj_;
        public:
        put() : multipart_method{}, offset_{}, file_obj_{} {
        }

        ~put() {
#ifdef RODS_SERVER
            if (file_obj_) {
                //const auto close_err = fileClose(file_obj_->comm(), file_obj_);
                //if (!close_err.ok()) {
                    //log(close_err);
                //}
            }
#endif
        }

        virtual size_t client_transfer(
                message_broker& _bro,
                std::shared_ptr<memory_mapped_file> _mmapped_file,
                const unipart_request& _uni_req,
                const size_t _bytes_remaining,
                const size_t _block_size) {

            //if this is a new part
            if (static_cast<size_t>(_uni_req.bytes_to_transfer) == _bytes_remaining) {
                offset_ = _uni_req.start_offset;
            }

            // read the data - block size or remainder
            /*
            zmq::message_t chunk_to_send{
                _mmapped_file + offset_,
                _bytes_remaining > _block_size ?
                    _block_size :
                    _bytes_remaining,
                nullptr};
                */

            message_broker::data_type chunk_to_send(_bytes_remaining > _block_size ?
                    _block_size :
                    _bytes_remaining);

            std::memcpy(chunk_to_send.data(), _mmapped_file->file_pointer(offset_), chunk_to_send.size());

            // ship the data to the server side
            _bro.send(chunk_to_send);

            // receive acknowledgement of data
            const auto server_response = _bro.receive();
            if(ACK_MSG != server_response) {
                std::cerr << "Received error from server: "
                        << server_response << std::endl;
                THROW(-1, boost::format("Received error from server: %s") % std::string(reinterpret_cast<const char*>(server_response.data()), server_response.size()));
            }
            offset_ = chunk_to_send.size();

            const auto new_bytes_remaining = _bytes_remaining - chunk_to_send.size();
            return new_bytes_remaining;
        }
        virtual void client_cleanup(
                message_broker& _bro) {

            // notify the server side that we are done transmitting parts
            unipart_request uni_req;
            uni_req.transfer_complete = true;

            _bro.send(uni_req);
            const auto rcv_msg = _bro.receive();

        }
        virtual size_t server_transfer(
                rsComm_t* _comm,
                message_broker& _bro,
                const unipart_request& _uni_req,
                const size_t _bytes_remaining,
                const size_t _block_size) {

#ifdef RODS_SERVER
            //if this is a new part
            if (static_cast<size_t>(_uni_req.bytes_to_transfer) == _bytes_remaining) {
                // open the file for write
                file_obj_ = boost::make_shared<file_object>(
                        _comm,
                        _uni_req.logical_path,
                        _uni_req.physical_path,
                        _uni_req.resource_hierarchy,
                        0, getDefFileMode(),
                        O_WRONLY | O_CREAT);

                // open the replica using the irods framework
                const auto open_err = fileOpen(file_obj_->comm(), file_obj_);
                if(!open_err.ok()) {
                    THROW(open_err.code(), open_err.result());
                }

            }


            //TODO: const this and the cast for write in 4.3
            auto chunk_received = _bro.receive<std::string>();

            // execute a write using the irods framework
            const auto write_err = fileWrite(file_obj_->comm(), file_obj_, const_cast<char*>(chunk_received.data()), chunk_received.size());
            if(!write_err.ok()) {
                _bro.send(ERROR_MSG);
                THROW(write_err.code(), write_err.result());
            }

            // acknowledge a successful write
            _bro.send(ACK_MSG);

            // track number of bytes written
            const auto new_bytes_remaining = _bytes_remaining - chunk_received.size();

            if (0 == new_bytes_remaining) {
                const auto close_err = fileClose(file_obj_->comm(), file_obj_);
                file_obj_.reset();
                if (!close_err.ok()) {
                    THROW(close_err.code(), close_err.result());
                }
            }

            return new_bytes_remaining;
#else
            return 0;
#endif
        }

        virtual void server_cleanup(
                rsComm_t*          _comm,
                resource_ptr _resc) {}

    };

    class get : public multipart_method {
        private:
            off_t offset_;
            file_object_ptr file_obj_;
        public:
        get() : multipart_method{},
                offset_{},
                file_obj_{} {
        }

        ~get() {
#ifdef RODS_SERVER
            if (file_obj_) {
                const auto close_err = fileClose(file_obj_->comm(), file_obj_);
                if (!close_err.ok()) {
                    log(close_err);
                }
            }
#endif
        }

        virtual size_t client_transfer(
                message_broker& _bro,
                std::shared_ptr<memory_mapped_file> _mmapped_file,
                const unipart_request& _uni_req,
                const size_t _bytes_remaining,
                const size_t _block_size) {

            //if this is a new part
            if (static_cast<size_t>(_uni_req.bytes_to_transfer) == _bytes_remaining) {
                offset_ = _uni_req.start_offset;
            }

            _bro.send(REQ_MSG);
            const auto chunk_received = _bro.receive();
            std::memcpy(_mmapped_file->file_pointer(offset_), chunk_received.data(), chunk_received.size());

            offset_ += chunk_received.size();

            const auto new_bytes_remaining = _bytes_remaining - chunk_received.size();
            return new_bytes_remaining;
        }

        virtual void client_cleanup(
                message_broker& _bro) {
            unipart_request uni_req;
            uni_req.transfer_complete = true;

            _bro.send(uni_req);
            const auto rcv_msg = _bro.receive();
        }

        virtual size_t server_transfer(
                rsComm_t*          _comm,
                message_broker& _bro,
                const unipart_request& _uni_req,
                const size_t _bytes_remaining,
                const size_t _block_size) {

#ifdef RODS_SERVER
            if ( !file_obj_ ) {

                file_obj_ = boost::make_shared<file_object>(
                        _comm,
                        _uni_req.logical_path,
                        _uni_req.physical_path,
                        _uni_req.resource_hierarchy,
                        0, getDefFileMode(),
                        O_RDONLY);

                // open the replica using the irods framework
                const auto open_err = fileOpen(file_obj_->comm(), file_obj_);
                if(!open_err.ok()) {
                    THROW(open_err.code(), open_err.result());
                }

            }

            if (static_cast<size_t>(_uni_req.bytes_to_transfer) == _bytes_remaining) {
                const auto lseek_err = fileLseek(file_obj_->comm(), file_obj_, _uni_req.start_offset, SEEK_SET);
                if(!lseek_err.ok()) {
                    THROW(lseek_err.code(), lseek_err.result());
                }
            }

            message_broker::data_type snd_msg(_bytes_remaining > _block_size ?
                    _block_size :
                    _bytes_remaining);

            // execute a read using the irods framework
            const auto read_err = fileRead(file_obj_->comm(), file_obj_, snd_msg.data(), snd_msg.size());
            if(!read_err.ok()) {
                THROW(read_err.code(), read_err.result());
            }

            const auto req_recv_msg = _bro.receive();
            _bro.send(snd_msg);

            /*
            // acknowledge a successful write
            const auto ack_rcv_msg = _bro.receive();
            if (ACK_MSG != ack_rcv_msg) {
                THROW(-1, boost::format{"Error message received from client: %s"} % std::string(reinterpret_cast<const char*>(ack_rcv_msg.data()), ack_rcv_msg.size()));
            }
            */

            // track number of bytes written
            const auto new_bytes_remaining = _bytes_remaining - snd_msg.size();

            return new_bytes_remaining;
#else
            return 0;
#endif
        }

        virtual void server_cleanup(
                rsComm_t*          _comm,
                resource_ptr _resc) {
        }

    };

}
#endif //DATA_TRANSFER_HPP__
