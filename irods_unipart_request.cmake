
set(
  AVRO_FILE
  irods_unipart_request
)

set(
  TARGET_FILE
  libapi_plugin_unipart 
)

file(MAKE_DIRECTORY "include")

add_custom_command(
   OUTPUT ${CMAKE_BINARY_DIR}/include/${AVRO_FILE}.hpp
   COMMAND ${IRODS_EXTERNALS_FULLPATH_AVRO}/bin/avrogencpp -n irods -o ${CMAKE_BINARY_DIR}/include/${AVRO_FILE}.hpp -i ${CMAKE_SOURCE_DIR}/avro_schemas/${AVRO_FILE}.json
   MAIN_DEPENDENCY ${CMAKE_SOURCE_DIR}/avro_schemas/${AVRO_FILE}.json
)

set_source_files_properties(
  ${CMAKE_SOURCE_DIR}/src/${TARGET_FILE}.cpp
  PROPERTIES
  OBJECT_DEPENDS ${CMAKE_BINARY_DIR}/include/${AVRO_FILE}.hpp
)

install(
  FILES
  ${CMAKE_BINARY_DIR}/include/${AVRO_FILE}.hpp
  DESTINATION usr/include/irods
)


