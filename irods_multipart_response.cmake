set(
    RESP_AVRO_FILE
  irods_multipart_response
)

set(
    RESP_TARGET_FILE
  libapi_plugin_multipart 
)


file(MAKE_DIRECTORY "${CMAKE_BINARY_DIR}/include")

add_custom_command(
    OUTPUT ${CMAKE_BINARY_DIR}/include/${RESP_AVRO_FILE}.hpp
    COMMAND ${IRODS_EXTERNALS_FULLPATH_AVRO}/bin/avrogencpp -n irods -o ${CMAKE_BINARY_DIR}/include/${RESP_AVRO_FILE}.hpp -i ${CMAKE_SOURCE_DIR}/avro_schemas/${RESP_AVRO_FILE}.json
    MAIN_DEPENDENCY ${CMAKE_SOURCE_DIR}/avro_schemas/${RESP_AVRO_FILE}.json
)

get_source_file_property(RESP_DEPS ${CMAKE_SOURCE_DIR}/src/${REQ_TARGET_FILE}.cpp OBJECT_DEPENDS)
if(${RESP_DEPS} STREQUAL "NOTFOUND")
    set(RESP_DEPS "")
endif()
list(APPEND RESP_DEPS ${CMAKE_BINARY_DIR}/include/${RESP_AVRO_FILE}.hpp)

set_source_files_properties(
    ${CMAKE_SOURCE_DIR}/src/${RESP_TARGET_FILE}.cpp
  PROPERTIES
  OBJECT_DEPENDS "${RESP_DEPS}"
)

install(
  FILES
  ${CMAKE_BINARY_DIR}/include/${RESP_AVRO_FILE}.hpp
  DESTINATION usr/include/irods
)


