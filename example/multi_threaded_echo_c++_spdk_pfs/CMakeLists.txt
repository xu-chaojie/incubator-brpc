cmake_minimum_required(VERSION 2.8.10)
project(multi_threaded_echo_c++ C CXX)

option(EXAMPLE_LINK_SO "Whether examples are linked dynamically" OFF)
set(DPDK_DIR "/usr/local/dpdk" CACHE PATH "DPDK dir")
set(SPDK_DIR "/usr/local/spdk" CACHE PATH "SPDK dir")
set(PFSD_DIR "/usr/local/curvestore/pfsd" CACHE PATH "pfsd dir")

include_directories(
    ${PFSD_DIR}/include
)

set(PFSD_LIB
    -L${PFSD_DIR}/lib
    -Wl,--push-state
    -Wl,--no-as-needed
    -Wl,--whole-archive
    -Bstatic
    pfsd_svr
    pfs
    -Wl,--no-whole-archive
    -Wl,--pop-state
)

include_directories(
    ${SPDK_DIR}/include
)

include_directories(
    ${DPDK_DIR}/include
)

link_directories(
    ${DPDK_DIR}/lib
)

set(DPDK_LIBS
 -Wl,--disable-new-dtags
 -Wl,--push-state
 -Wl,--whole-archive
 -Wl,--no-as-needed
 -Wl,--start-group
 rte_bus_pci
 rte_cryptodev
 rte_dmadev
 rte_eal
 rte_ethdev
 rte_hash
 rte_kvargs
 rte_mbuf
 rte_mempool
 rte_mempool_ring
 rte_net
 rte_pci
 rte_power
 rte_rcu
 rte_ring
 rte_telemetry
 rte_vhost
 rte_meter
 rte_timer
 rte_stack
 -Wl,--end-group
 -Wl,--no-whole-archive
 -Wl,--pop-state
)

set(SPDK_LIBS
 -L${SPDK_DIR}/lib
 -Wl,--disable-new-dtags
 -Wl,--push-state
 -Wl,--no-as-needed
 -Wl,--whole-archive
 -Wl,--start-group
 -Bstatic
 spdk_bdev_aio
 spdk_bdev_null
 spdk_bdev_nvme
 spdk_nvme
 spdk_env_dpdk
 spdk_sock_posix
 spdk_event
 spdk_event_bdev
 spdk_bdev
 spdk_notify
 spdk_dma
 spdk_event_accel
 spdk_accel
 spdk_event_vmd
 spdk_vmd
 spdk_event_sock
 spdk_init
 spdk_thread
 spdk_trace
 spdk_sock
 spdk_rpc
 spdk_jsonrpc
 spdk_json
 spdk_util
 spdk_log
 -Wl,--end-group
 -Wl,--no-whole-archive
)

set(SPDK_DEPS rt uuid numa crypto bsd m aio dl)

execute_process(
    COMMAND bash -c "find ${PROJECT_SOURCE_DIR}/../.. -type d -regex \".*output/include$\" | head -n1 | xargs dirname | tr -d '\n'"
    OUTPUT_VARIABLE OUTPUT_PATH
)

set(CMAKE_PREFIX_PATH ${OUTPUT_PATH})

include(FindThreads)
include(FindProtobuf)
protobuf_generate_cpp(PROTO_SRC PROTO_HEADER echo.proto)
# include PROTO_HEADER
include_directories(${CMAKE_CURRENT_BINARY_DIR})

# Search for libthrift* by best effort. If it is not found and brpc is
# compiled with thrift protocol enabled, a link error would be reported.
find_library(THRIFT_LIB NAMES thrift)
if (NOT THRIFT_LIB)
    set(THRIFT_LIB "")
endif()
find_library(THRIFTNB_LIB NAMES thriftnb)
if (NOT THRIFTNB_LIB)
    set(THRIFTNB_LIB "")
endif()

#find_path(GPERFTOOLS_INCLUDE_DIR NAMES gperftools/heap-profiler.h)
#find_library(GPERFTOOLS_LIBRARIES NAMES tcmalloc_and_profiler)
#include_directories(${GPERFTOOLS_INCLUDE_DIR})

find_path(BRPC_INCLUDE_PATH NAMES brpc/server.h)
if(EXAMPLE_LINK_SO)
    find_library(BRPC_LIB NAMES brpc)
else()
    find_library(BRPC_LIB NAMES libbrpc.a brpc)
endif()
if((NOT BRPC_INCLUDE_PATH) OR (NOT BRPC_LIB))
    message(FATAL_ERROR "Fail to find brpc")
endif()
include_directories(${BRPC_INCLUDE_PATH})

find_path(GFLAGS_INCLUDE_PATH gflags/gflags.h)
find_library(GFLAGS_LIBRARY NAMES gflags libgflags)
if((NOT GFLAGS_INCLUDE_PATH) OR (NOT GFLAGS_LIBRARY))
    message(FATAL_ERROR "Fail to find gflags")
endif()
include_directories(${GFLAGS_INCLUDE_PATH})

execute_process(
    COMMAND bash -c "grep \"namespace [_A-Za-z0-9]\\+ {\" ${GFLAGS_INCLUDE_PATH}/gflags/gflags_declare.h | head -1 | awk '{print $2}' | tr -d '\n'"
    OUTPUT_VARIABLE GFLAGS_NS
)
if(${GFLAGS_NS} STREQUAL "GFLAGS_NAMESPACE")
    execute_process(
        COMMAND bash -c "grep \"#define GFLAGS_NAMESPACE [_A-Za-z0-9]\\+\" ${GFLAGS_INCLUDE_PATH}/gflags/gflags_declare.h | head -1 | awk '{print $3}' | tr -d '\n'"
        OUTPUT_VARIABLE GFLAGS_NS
    )
endif()
if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    include(CheckFunctionExists)
    CHECK_FUNCTION_EXISTS(clock_gettime HAVE_CLOCK_GETTIME)
    if(NOT HAVE_CLOCK_GETTIME)
        set(DEFINE_CLOCK_GETTIME "-DNO_CLOCK_GETTIME_IN_MAC")
    endif()
endif()

if (${CMAKE_SYSTEM_PROCESSOR} STREQUAL "x86_64")                                
    set(CPU_FLAGS "-mssse3")
endif()

set(CMAKE_CPP_FLAGS "${DEFINE_CLOCK_GETTIME} -DGFLAGS_NS=${GFLAGS_NS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CPP_FLAGS} -DNDEBUG -D__const__= -pipe -W -Wall -Wno-unused-parameter -fPIC -fno-omit-frame-pointer")
set(CMAKE_CPP_FLAGS "${DEFINE_CLOCK_GETTIME} ${CPU_FLAGS} -DBRPC_WITH_DPDK=${WITH_DPDK_VAL} -DGFLAGS_NS=${GFLAGS_NS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CPP_FLAGS} -DNDEBUG -O2 -D__const__= -pipe -W -Wall -Wno-unused-parameter -fPIC -fno-omit-frame-pointer")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DBRPC_ENABLE_CPU_PROFILER")

if(CMAKE_VERSION VERSION_LESS "3.1.3")
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
    endif()
    if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
    endif()
else()
    set(CMAKE_CXX_STANDARD 11)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
endif()

find_path(LEVELDB_INCLUDE_PATH NAMES leveldb/db.h)
find_library(LEVELDB_LIB NAMES leveldb)
if ((NOT LEVELDB_INCLUDE_PATH) OR (NOT LEVELDB_LIB))
    message(FATAL_ERROR "Fail to find leveldb")
endif()
include_directories(${LEVELDB_INCLUDE_PATH})

find_library(SSL_LIB NAMES ssl)
if (NOT SSL_LIB)
    message(FATAL_ERROR "Fail to find ssl")
endif()

find_library(CRYPTO_LIB NAMES crypto)
if (NOT CRYPTO_LIB)
    message(FATAL_ERROR "Fail to find crypto")
endif()

include_directories(/usr/local/ucx/include)
link_directories(/usr/local/ucx/lib)
set(UCX_LIB ucp uct ucm ucs ucs_signal)
set(DYNAMIC_LIB
    ${UCX_LIB}
    ${CMAKE_THREAD_LIBS_INIT}
    ${GFLAGS_LIBRARY}
    ${PROTOBUF_LIBRARIES}
    ${LEVELDB_LIB}
    ${SSL_LIB}
    ${CRYPTO_LIB}
    ${THRIFT_LIB}
    ${THRIFTNB_LIB}
    dl
    )

if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(DYNAMIC_LIB ${DYNAMIC_LIB}
        pthread
        "-framework CoreFoundation"
        "-framework CoreGraphics"
        "-framework CoreData"
        "-framework CoreText"
        "-framework Security"
        "-framework Foundation"
        "-Wl,-U,_MallocExtension_ReleaseFreeMemory"
        "-Wl,-U,_ProfilerStart"
        "-Wl,-U,_ProfilerStop")
endif()

add_executable(multi_threaded_echo_client client.cpp ${PROTO_SRC} ${PROTO_HEADER})
add_executable(multi_threaded_echo_server server.cpp ${PROTO_SRC} ${PROTO_HEADER})

target_link_libraries(multi_threaded_echo_client ${BRPC_LIB} ${SPDK_LIBS} ${SPDK_DEPS} ${DPDK_LIBS} ${DYNAMIC_LIB}) # ${GPERFTOOLS_LIBRARIES})
target_link_libraries(multi_threaded_echo_server ${BRPC_LIB} ${PFSD_LIB} ${SPDK_LIBS} ${SPDK_DEPS} ${DPDK_LIBS} ${DYNAMIC_LIB}) # ${GPERFTOOLS_LIBRARIES})
