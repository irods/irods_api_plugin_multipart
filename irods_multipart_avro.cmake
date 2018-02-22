set(
    MULTIPART_AVRO_FILE
  irods_multipart_avro_types
)

set(
    MULTIPART_TARGET_FILE
  libapi_plugin_multipart
)


file(MAKE_DIRECTORY "${CMAKE_BINARY_DIR}/include")

add_custom_command(
    OUTPUT ${CMAKE_BINARY_DIR}/include/${MULTIPART_AVRO_FILE}.hpp
    COMMAND ${IRODS_EXTERNALS_FULLPATH_AVRO}/bin/avrogencpp -n irods -o ${CMAKE_BINARY_DIR}/include/${MULTIPART_AVRO_FILE}.hpp -i ${CMAKE_SOURCE_DIR}/avro_schemas/${MULTIPART_AVRO_FILE}.json
    MAIN_DEPENDENCY ${CMAKE_SOURCE_DIR}/avro_schemas/${MULTIPART_AVRO_FILE}.json
)

get_source_file_property(MULTIPART_DEPS ${CMAKE_SOURCE_DIR}/src/${MULTIPART_TARGET_FILE}.cpp OBJECT_DEPENDS)
list(APPEND MULTIPART_DEPS ${CMAKE_BINARY_DIR}/include/${MULTIPART_AVRO_FILE}.hpp)
list(REMOVE_DUPLICATES MULTIPART_DEPS)
list(REMOVE_ITEM MULTIPART_DEPS "NOTFOUND")

set_source_files_properties(
    ${CMAKE_SOURCE_DIR}/src/${MULTIPART_TARGET_FILE}.cpp
  PROPERTIES
  OBJECT_DEPENDS "${MULTIPART_DEPS}"
)

install(
  FILES
  ${CMAKE_BINARY_DIR}/include/${MULTIPART_AVRO_FILE}.hpp
  DESTINATION usr/include/irods
)


