cmake_minimum_required(VERSION 3.5)
set(CMAKE_CXX_STANDARD 17)
project(librdkafka_issue_demo)

include(ExternalProject)
include(FetchContent)
include(FindOpenSSL)
include(FindZLIB)
include(FindThreads)
include(CTest)
find_package(OpenSSL REQUIRED)

message("CMAKE_CXX_COMPILER_ID is ${CMAKE_CXX_COMPILER_ID}")
if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
   add_compile_options (-fdiagnostics-color=always)
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
   add_compile_options (-fcolor-diagnostics)
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "AppleClang")
   add_compile_options (-fcolor-diagnostics)
endif ()

if(WIN32)
    set(BYPRODUCT "lib/rdkafka.lib")
else()
    set(BYPRODUCT "lib/librdkafka.a")
endif()

ExternalProject_Add(
  librdkafka-external
  URL "https://github.com/edenhill/librdkafka/archive/v1.5.0.tar.gz"
  URL_HASH "SHA256=f7fee59fdbf1286ec23ef0b35b2dfb41031c8727c90ced6435b8cf576f23a656"
  CMAKE_ARGS ${PASSTHROUGH_CMAKE_ARGS}
    "-DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR}/thirdparty/librdkafka-install"
    "-DCMAKE_BUILD_TYPE=Debug"
    "-DWITH_SSL=ON"
    "-DWITH_SASL=OFF"
    "-DRDKAFKA_BUILD_STATIC=ON"
    "-DRDKAFKA_BUILD_EXAMPLES=OFF"
    "-DRDKAFKA_BUILD_TESTS=OFF"
    "-DENABLE_LZ4_EXT=OFF"
    "-DWITH_ZSTD=OFF"
    "-DCMAKE_MODULE_PATH=${CMAKE_MODULE_PATH_PASSTHROUGH_LIST}"
    "-DCMAKE_C_FLAGS=${CURL_C_FLAGS}"
    "-DCMAKE_INSTALL_LIBDIR=lib"
    "-DCMAKE_CXX_FLAGS=${CURL_CXX_FLAGS}"
  BUILD_BYPRODUCTS "${CMAKE_CURRENT_BINARY_DIR}/thirdparty/librdkafka-install/${BYPRODUCT}"
  EXCLUDE_FROM_ALL TRUE)

add_dependencies(librdkafka-external OpenSSL::SSL OpenSSL::Crypto ZLIB::ZLIB)

set(LIBRDKAFKA_FOUND "YES" CACHE STRING "" FORCE)
set(LIBRDKAFKA_INCLUDE_DIR "${CMAKE_CURRENT_BINARY_DIR}/thirdparty/librdkafka-install/include" CACHE STRING "" FORCE)
set(LIBRDKAFKA_LIBRARY "${CMAKE_CURRENT_BINARY_DIR}/thirdparty/librdkafka-install/${BYPRODUCT}" CACHE STRING "" FORCE)
set(LIBRDKAFKA_LIBRARIES ${LIBRDKAFKA_LIBRARY} CACHE STRING "" FORCE)
file(MAKE_DIRECTORY ${LIBRDKAFKA_INCLUDE_DIR}/librdkafka)

add_library(librdkafka STATIC IMPORTED)
set_target_properties(librdkafka PROPERTIES IMPORTED_LOCATION "${LIBRDKAFKA_LIBRARY}")
add_dependencies(librdkafka librdkafka-external)
set_property(TARGET librdkafka APPEND PROPERTY INTERFACE_LINK_LIBRARIES OpenSSL::SSL OpenSSL::Crypto ZLIB::ZLIB Threads::Threads)
set_property(TARGET librdkafka APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${LIBRDKAFKA_INCLUDE_DIR})
set_property(TARGET librdkafka APPEND PROPERTY INTERFACE_COMPILE_DEFINITIONS "LIBRDKAFKA_STATICLIB=1")

set(CATCH2_CMAKE_ARGS ${PASSTHROUGH_CMAKE_ARGS}
  "-DCATCH_BUILD_TESTING=OFF"
  "-DBUILD_TESTING=OFF"
  "-DCATCH_BUILD_EXAMPLES=OFF"
  "-DCATCH_INSTALL_DOCS=OFF"
  "-DCATCH_INSTALL_HELPERS=OFF")

FetchContent_Declare(
  Catch2
  URL "https://github.com/catchorg/Catch2/archive/v2.13.2.tar.gz"
  URL_HASH "SHA256=5e39d9199f4f174dc3c8896fb4cf0a2ce9b9c358ae759b87fade6d615ca2d27e"
)

FetchContent_MakeAvailable(Catch2)

file(GLOB_RECURSE SOURCE_FILES src/*.cpp)
file(GLOB_RECURSE HEADER_FILES src/*.h)
add_library(librdkafka_issue_demo_lib STATIC ${SOURCE_FILES} ${HEADER_FILES})
target_include_directories(librdkafka_issue_demo_lib PUBLIC include/ ${LIBRDKAFKA_INCLUDE_DIR})
target_link_libraries(librdkafka_issue_demo_lib debug librdkafka)

file(GLOB_RECURSE TEST_FILES test/*.cpp)
add_executable(librdkafka_issue_demo ${TEST_FILES})
target_link_libraries(librdkafka_issue_demo librdkafka_issue_demo_lib Catch2::Catch2 librdkafka)

add_test(NAME Test_librdkafka COMMAND "librdkafka_issue_demo" WORKING_DIRECTORY ${TEST_DIR})
