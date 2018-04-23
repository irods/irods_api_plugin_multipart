#ifndef IRODS_MEMORY_MAPPED_FILE_HPP__
#define IRODS_MEMORY_MAPPED_FILE_HPP__

#include <sys/mman.h>
#include <unistd.h>
#include <sys/types.h>
#include <string>
#include <cstdio>

namespace irods {
    enum file_access_t { R, W, RW };

    class memory_mapped_file {
        private:
            static int
                open_file(
                        const std::string& _file_path,
                        const file_access_t _file_access_type,
                        const mode_t _file_mode) {
                    const auto open_flags = [_file_access_type]() {
                        switch(_file_access_type) {
                            case file_access_t::R :
                                return O_RDONLY;
                            case file_access_t::W :
                                return O_WRONLY | O_CREAT;
                            case file_access_t::RW :
                                return O_RDWR | O_CREAT;
                        }
                    }();
                    const auto fd = open(
                            _file_path.c_str(),
                            open_flags,
                            _file_mode);
                    if (-1 == fd) {
                        THROW(UNIX_FILE_OPEN_ERR, boost::format("Failed to open file at: [%s]") % _file_path);
                    }
                    return fd;
                }

            static off_t
                resolve_file_size(
                        const int _fd,
                        const std::string& _file_path,
                        const file_access_t _file_access_type,
                        const off_t _file_size) {
                    struct stat stat_buf;
                    if (-1 == fstat(_fd, &stat_buf)) {
                        THROW(UNIX_FILE_STAT_ERR, boost::format("Failed to stat file at: [%s]") % _file_path);
                    }

                    if (-1 == _file_size || _file_size == stat_buf.st_size) {
                        //file matches requested file size or no
                        //file size requested; nothing to do
                        return stat_buf.st_size;
                    } else {
                        switch (_file_access_type) {
                            case R:
                                //read does not support changing the file_size
                                THROW(SYS_INVALID_INPUT_PARAM, "Cannot set file_size on a file opened for read.");
                            case W:
                            case RW:
                                auto truncate_err = ftruncate(_fd, _file_size);
                                if (-1 == truncate_err) {
                                    THROW(UNIX_FILE_TRUNCATE_ERR, boost::format("Failed to truncate file at: [%s] to [%d] bytes.") % _file_path % _file_size);
                                }
                                return _file_size;
                        }
                    }
                }

            static void*
                mmap_file(
                        const int _fd,
                        const std::string& _file_path,
                        const file_access_t _file_access_type,
                        const off_t _file_size) {
                    const auto mmap_prot = [_file_access_type]() {
                        switch (_file_access_type) {
                            case R:
                                return PROT_READ;
                            case W:
                                return PROT_WRITE;
                            case RW:
                                return PROT_READ | PROT_WRITE;
                        }
                    }();
                    auto mmapped_file = mmap(nullptr, _file_size, mmap_prot, MAP_SHARED, _fd, 0);
                    if (MAP_FAILED == mmapped_file) {
                        THROW(SYS_INTERNAL_ERR, boost::format("Failed to mmap file: [%s]") % _file_path);
                    }
                    return mmapped_file;
                }

        public:
            const file_access_t file_access_type_;
            std::string file_path_;
            const int file_descriptor_;
            const off_t file_size_;
            void * const start_of_file_;

            memory_mapped_file(
                    const std::string& _file_path,
                    const file_access_t _file_access_type,
                    const mode_t _file_mode,
                    const off_t _file_size = -1) :
                file_access_type_{_file_access_type},
                file_path_{_file_path},
                file_descriptor_{open_file(
                        file_path_,
                        file_access_type_,
                        _file_mode)},
                file_size_{resolve_file_size(
                        file_descriptor_,
                        _file_path,
                        file_access_type_,
                        _file_size)},
                start_of_file_{mmap_file(
                        file_descriptor_,
                        _file_path,
                        file_access_type_,
                        file_size_)} {
                }

            ~memory_mapped_file() {
                munmap(start_of_file_, file_size_);
                close(file_descriptor_);
            }

            char*
                file_pointer(const off_t _offset = 0) {
                    return file_pointer<char>(_offset);
                }

            template<typename T>
            T*
                file_pointer(const off_t _offset = 0) {
                    return static_cast<T*>(start_of_file_) + _offset;
                }

            void
                rename(const std::string& _destination_filepath) {
                    if ( std::rename(file_path_.c_str(), _destination_filepath.c_str()) == -1 ) {
                        THROW(UNIX_FILE_RENAME_ERR, boost::format("Error occurred during rename of mmapped_file [%s]") % file_path_.c_str());
                    } else {
                        file_path_ = _destination_filepath;
                    }
                }

    };
}

#endif // IRODS_MEMORY_MAPPED_FILE_HPP__
