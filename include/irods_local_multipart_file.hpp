#ifndef LOCAL_MULTIPART_FILE_HPP__
#define LOCAL_MULTIPART_FILE_HPP__
#include <string>
#include <cstring>
#include <algorithm>
#include <numeric>

#include "irods_memory_mapped_file.hpp"
#include "irods_multipart_avro_types.hpp"

namespace irods {

    class local_multipart_file {
        public:
        const off_t data_object_size_;
        const size_t number_of_parts_;
        const size_t block_size_;
        const std::string data_object_name_;
        const std::string local_filename_;
        const time_t timestamp_;

        const size_t number_of_blocks_;
        const size_t blocks_per_part_quotient_;
        const size_t blocks_per_part_remainder_;

        private:
        std::unique_ptr<memory_mapped_file> file_;

        public:

        static std::string
            get_name_for_in_progress_multipart_file(
                    const std::string& _data_object_name,
                    const std::time_t _timestamp,
                    const off_t _data_object_size,
                    const size_t _number_of_parts,
                    const size_t _block_size) {
                std::stringstream multipart_filename{};
                const std::string suffix{".multipart"};

                multipart_filename <<
                    _timestamp << "_" <<
                    _data_object_size << "_" <<
                    _number_of_parts << "_" <<
                    _block_size << "_" <<
                    _data_object_name << suffix;

                return multipart_filename.str();
            }

        static off_t
            get_part_progress_offset(
                    const off_t _data_object_size) {
                auto offset_from_alignment = _data_object_size % alignof(off_t);
                return _data_object_size +
                    (!!offset_from_alignment) * offset_from_alignment;
            }

        static off_t
            get_size_for_in_progress_multipart_file(
                    const off_t _data_object_size,
                    const size_t _number_of_parts) {
                return get_part_progress_offset(_data_object_size) +
                    _number_of_parts * sizeof(off_t);
            }

        std::unique_ptr<memory_mapped_file>
            get_file_pointer(
                    const std::string& _local_filename,
                    const std::string& _data_object_name,
                    const std::time_t _timestamp,
                    const off_t _data_object_size,
                    const size_t _number_of_parts,
                    const size_t _block_size,
                    const irods::multipart_operation_t _multipart_operation) {
                    switch(_multipart_operation) {
                        case irods::multipart_operation_t::GET:
                            return std::make_unique<memory_mapped_file>(
                                    get_name_for_in_progress_multipart_file(
                                        _data_object_name,
                                        _timestamp,
                                        _data_object_size,
                                        _number_of_parts,
                                        _block_size),
                                    file_access_t::RW,
                                    mode_t{0600},
                                    get_size_for_in_progress_multipart_file(
                                        data_object_size_,
                                        number_of_parts_));
                        case irods::multipart_operation_t::PUT:
                            return std::make_unique<memory_mapped_file>(
                                    _local_filename,
                                    file_access_t::R,
                                    mode_t{0400},
                                    _data_object_size);
                    }
            }

        local_multipart_file(
                const std::string& _local_filename,
                const std::string& _data_object_name,
                const std::time_t _timestamp,
                const off_t _data_object_size,
                const size_t _number_of_parts,
                const size_t _block_size,
                const irods::multipart_operation_t _multipart_operation) :
            data_object_size_{_data_object_size},
            number_of_parts_{_number_of_parts},
            block_size_{_block_size},
            data_object_name_{_data_object_name},
            local_filename_{_local_filename},
            timestamp_{_timestamp},
            number_of_blocks_{data_object_size_ / block_size_ +
                !!(data_object_size_ % block_size_)},
            blocks_per_part_quotient_{number_of_blocks_ / number_of_parts_},
            blocks_per_part_remainder_{number_of_blocks_ % number_of_parts_},
            file_{get_file_pointer(
                        local_filename_,
                        data_object_name_,
                        timestamp_,
                        data_object_size_,
                        number_of_parts_,
                        block_size_,
                        _multipart_operation)
            } {
            }

        size_t
            get_part_number_from_block_number(size_t _block_number) {
                if ( blocks_per_part_quotient_ == 0 ) {
                    return _block_number;
                } else {
                    const auto bonus_blocks = std::min(
                            blocks_per_part_remainder_,
                            (_block_number + 1) / (blocks_per_part_quotient_ + 1 )
                            );

                    return (_block_number - bonus_blocks) / blocks_per_part_quotient_;
                }
            }

        size_t
            get_beginning_block_number_from_part_number(
                    size_t _part_number) {
                const auto bonus_blocks = std::min(
                        _part_number,
                        blocks_per_part_remainder_);
                return _part_number * blocks_per_part_quotient_ + bonus_blocks;
            }

        char*
            get_pointer_to_contents() {
                return file_->file_pointer();
            }

        size_t*
            get_pointer_to_part_progress() {
                return reinterpret_cast<size_t*>(file_->file_pointer() +
                        get_part_progress_offset(data_object_size_));
            }

        size_t
            get_bytes_already_transferred_for_part(
                    const size_t _part_number,
                    const size_t _part_size,
                    const size_t _bytes_already_transferred,
                    const irods::multipart_operation_t _multipart_operation) {
                switch (_multipart_operation) {
                    case irods::multipart_operation_t::PUT:
                        return _bytes_already_transferred;
                    case irods::multipart_operation_t::GET:
                        const size_t bytes_already_transferred = get_pointer_to_part_progress()[_part_number] * block_size_;
                        return std::min(bytes_already_transferred, _part_size);
                }
            }

        void
            write(
                    const void* _to_write,
                    size_t _number_of_bytes,
                    off_t _offset) {
                std::memcpy(get_pointer_to_contents() + _offset,
                        _to_write,
                        _number_of_bytes);
                const auto block_number = _offset / block_size_;
                const auto part_number = get_part_number_from_block_number(block_number);
                get_pointer_to_part_progress()[part_number] = block_number - get_beginning_block_number_from_part_number(part_number) + 1;
            }

        void
            read(
                    void* _to_read_into,
                    size_t _number_of_bytes,
                    off_t _offset) {
                std::memcpy(_to_read_into,
                        get_pointer_to_contents() + _offset,
                        _number_of_bytes);
            }

        void
            finalize() {
                if (file_->file_access_type_ == file_access_t::RW &&
                        file_->file_size_ != data_object_size_) {
                    const auto blocks_written = std::accumulate(
                            get_pointer_to_part_progress(),
                            get_pointer_to_part_progress() + number_of_parts_,
                            size_t{0});
                    std::cout << "blocks written:   " << blocks_written << std::endl <<
                                 "number of blocks: " << number_of_blocks_ << std::endl;
                    if (number_of_blocks_ == blocks_written) {
                        ftruncate(file_->file_descriptor_, data_object_size_);
                        file_->rename(local_filename_);
                    }
                }
            }

    };
}

#endif //LOCAL_MULTIPART_FILE_HPP__
