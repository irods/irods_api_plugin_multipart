set(
  IRODS_API_PLUGIN_SOURCES_api_plugin_multipart_server
  ${CMAKE_SOURCE_DIR}/src/libapi_plugin_multipart.cpp
  )

set(
  IRODS_API_PLUGIN_SOURCES_api_plugin_multipart_client
  ${CMAKE_SOURCE_DIR}/src/libapi_plugin_multipart.cpp
  )

set(
  IRODS_API_PLUGIN_COMPILE_DEFINITIONS_api_plugin_multipart_server
  RODS_SERVER
  ENABLE_RE
  )

set(
  IRODS_API_PLUGIN_COMPILE_DEFINITIONS_api_plugin_multipart_client
  )

set(
  IRODS_API_PLUGIN_LINK_LIBRARIES_api_plugin_multipart_server
  irods_api_endpoint
  irods_client
  irods_server
  irods_common
  irods_plugin_dependencies
  )

set(
  IRODS_API_PLUGIN_LINK_LIBRARIES_api_plugin_multipart_client
  irods_api_endpoint
  irods_client
  irods_common
  irods_plugin_dependencies
  )

set(
  IRODS_API_PLUGINS
  api_plugin_multipart_server
  api_plugin_multipart_client
  )

file(MAKE_DIRECTORY "include")

add_custom_command(
    OUTPUT ${CMAKE_BINARY_DIR}/include/irods_multipart_request.hpp
    COMMAND ${IRODS_EXTERNALS_FULLPATH_AVRO}/bin/avrogencpp -n irods -o ${CMAKE_BINARY_DIR}/include/irods_multipart_request.hpp -i ${CMAKE_SOURCE_DIR}/avro_schemas/irods_multipart_request.json
    MAIN_DEPENDENCY ${CMAKE_SOURCE_DIR}/avro_schemas/irods_multipart_request.json
)

set_source_files_properties(
   ${CMAKE_SOURCE_DIR}/src/libapi_plugin_multipart.cpp
   PROPERTIES
   OBJECT_DEPENDS ${CMAKE_BINARY_DIR}/include/irods_multipart_request.hpp
)

install(
     FILES
     ${CMAKE_BINARY_DIR}/include/irods_multipart_request.hpp
     DESTINATION usr/include/irods
    )



foreach(PLUGIN ${IRODS_API_PLUGINS})
  add_library(
    ${PLUGIN}
    MODULE
    ${IRODS_API_PLUGIN_SOURCES_${PLUGIN}}
    )

  target_include_directories(
    ${PLUGIN}
    PRIVATE
    ${CMAKE_BINARY_DIR}/include
    ${CMAKE_SOURCE_DIR}/include
    ${IRODS_INCLUDE_DIRS}
    ${IRODS_EXTERNALS_FULLPATH_BOOST}/include
    ${IRODS_EXTERNALS_FULLPATH_JANSSON}/include
    ${IRODS_EXTERNALS_FULLPATH_ARCHIVE}/include
    ${IRODS_EXTERNALS_FULLPATH_AVRO}/include
    ${IRODS_EXTERNALS_FULLPATH_ZMQ}/include
    ${IRODS_EXTERNALS_FULLPATH_CPPZMQ}/include
    )

  target_link_libraries(
    ${PLUGIN}
    PRIVATE
    ${IRODS_API_PLUGIN_LINK_LIBRARIES_${PLUGIN}}
    ${IRODS_EXTERNALS_FULLPATH_AVRO}/lib/libavrocpp.so
    ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_filesystem.so
    ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_system.so
    ${IRODS_EXTERNALS_FULLPATH_ARCHIVE}/lib/libarchive.so
    ${IRODS_EXTERNALS_FULLPATH_ZMQ}/lib/libzmq.so
    ${OPENSSL_CRYPTO_LIBRARY}
    ${CMAKE_DL_LIBS}
    )

  target_compile_definitions(${PLUGIN} PRIVATE ${IRODS_API_PLUGIN_COMPILE_DEFINITIONS_${PLUGIN}} ${IRODS_COMPILE_DEFINITIONS} BOOST_SYSTEM_NO_DEPRECATED)
  target_compile_options(${PLUGIN} PRIVATE -Wno-write-strings)
  set_property(TARGET ${PLUGIN} PROPERTY CXX_STANDARD ${IRODS_CXX_STANDARD})

  install(
    TARGETS
    ${PLUGIN}
    LIBRARY
    DESTINATION usr/lib/irods/plugins/api_v5
    )
endforeach()
