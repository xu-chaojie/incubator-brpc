// Copyright (c) 2014 Baidu, Inc.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// A server to receive EchoRequest and send back EchoResponse.

#include <gflags/gflags.h>
#include <butil/logging.h>
#include <butil/iobuf.h>
#include <brpc/server.h>
#include <uct/api/uct.h>
#include <brpc/ucp_cm.h>

#include "echo.pb.h"

#if BRPC_WITH_DPDK
#include <rte_eal.h>
#include <rte_errno.h>
#include <rte_thread.h>
#include <rte_malloc.h>
#endif

#include <err.h>

#include <pfs_api.h>
#include <pfsd.h>
#include <pfsd_sdk.h> 
#include <pfsd_sdk_log.h>
#include <pfs_spdk_api.h>
#include <pfs_trace_func.h>
#include <pfs_option_api.h>

DEFINE_bool(use_dpdk_malloc, true, "use dpdk malloc");
DEFINE_bool(echo_attachment, true, "Echo attachment as well");
DEFINE_int32(port, 8002, "TCP Port of this server");
DEFINE_int32(idle_timeout_s, -1, "Connection will be closed if there is no "
             "read/write operations during the last `idle_timeout_s'");
DEFINE_int32(logoff_ms, 2000, "Maximum duration of server's LOGOFF state "
             "(waiting for client to close connection before server stops)");
DEFINE_int32(max_concurrency, 0, "Limit of request processing in parallel");
DEFINE_int32(internal_port, -1, "Only allow builtin services at this port");
DEFINE_bool(enable_ucp, true, "Enable ucp port");
DEFINE_string(ucp_address, "0.0.0.0", "Ucp listener address");
DEFINE_int32(ucp_port, 13339, "Ucp listener port");
 
#define SHM_DIR "/dev/shm/pfsd"

DEFINE_bool(daemon, false, "become daemon process");
DEFINE_int32(server_id, 0, "PFSD server id");
DEFINE_string(pbd_name, "", "PBD name");
DEFINE_string(shm_dir, SHM_DIR, "pfsd shared memory dir");
DEFINE_int32(pollers, 2, "PFSD pollers");
DEFINE_int32(workers, 50, "PFSD pollers");
DEFINE_string(spdk_nvme_controller, "", "SPDK nvme controller");
DEFINE_string(spdk_rpc_address, "/tmp/pfs_spdk.sock", "SPDK rpc address");
DEFINE_int32(pfs_spdk_driver_poll_delay, 0, "spdk driver poller delay");

void set_ucp_callback()
{
	struct pfs_spdk_driver_poller poller;

	brpc::get_or_create_ucp_cm();
	poller.register_callback = brpc::ucp_register_worker_callback;
	poller.notify_callback = brpc::ucp_notify_worker_callback;
	poller.remove_callback = brpc::ucp_remove_worker_callback;
	pfs_spdk_set_driver_poller(&poller);
}

void set_pfs_options()
{
	pfs_option_set_int("daemon", FLAGS_daemon);
	pfs_option_set_int("server_id", FLAGS_server_id);
	pfs_option_set("pbd_name", FLAGS_pbd_name.c_str());
	pfs_option_set("shm_dir", FLAGS_shm_dir.c_str());
	pfs_option_set_int("pollers", FLAGS_pollers);
	pfs_option_set_int("workers", FLAGS_workers);
	pfs_option_set("spdk_nvme_controller", FLAGS_spdk_nvme_controller.c_str());
	pfs_option_set("spdk_rpc_address", FLAGS_spdk_rpc_address.c_str());
	pfs_option_set_int("pfs_spdk_driver_poll_delay", FLAGS_pfs_spdk_driver_poll_delay);
}


namespace example {
// Your implementation of EchoService
class EchoServiceImpl : public EchoService {
public:
    EchoServiceImpl() {}
    ~EchoServiceImpl() {};
    void Echo(google::protobuf::RpcController* cntl_base,
              const EchoRequest* request,
              EchoResponse* response,
              google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl =
            static_cast<brpc::Controller*>(cntl_base);

        // Echo request and its attachment
        response->set_message(request->message());
        auto &a = cntl->response_attachment();
        if (!a.empty()) {
            const auto &b = a.backing_block(0);
            if (!pfs_is_spdk_mem((void *)b.data(), b.size())) {
                fprintf(stderr, "not spdk memory\n");
            }
        }
        if (FLAGS_echo_attachment) {
            cntl->response_attachment().append(cntl->request_attachment());
        }
    }
};
}  // namespace example

DEFINE_bool(h, false, "print help information");

#if BRPC_WITH_DPDK

void
unaffinitize_thread(void)
{
    rte_cpuset_t new_cpuset;
    long num_cores, i;

    CPU_ZERO(&new_cpuset);

    num_cores = sysconf(_SC_NPROCESSORS_CONF);

    /* Create a mask containing all CPUs */
    for (i = 0; i < num_cores; i++) {
         CPU_SET(i, &new_cpuset);
    }
    rte_thread_set_affinity(&new_cpuset);
}

void* dpdk_mem_allocate(size_t align, size_t sz)
{
    /* rte_malloc seems fast enough, otherwise we need to use mempool */
    // XXX use spdk_malloc instead
    //void *p=spdk_malloc(sz, align, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    return rte_malloc("iobuf", sz, align);
}

void dpdk_mem_free(void* p, size_t sz)
{
    // XXX use spdk_free instead
    rte_free(p);
}

int dpdk_for_uct_alloc(void **address, size_t align,
                      size_t length, const char *name)
{
    char buf[128];
    void *p = rte_malloc(name, length, align);
    //void *p=spdk_malloc(length, align, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    if (p) {
        *address = p;
        snprintf(buf, sizeof(buf),
            "%s, sucess allocated: %p, align: %ld, length:%ld, name:%s\n", 
            __func__, p, align, length, name);
        LOG(INFO) << buf;
        return 0;
    }
    snprintf(buf, sizeof(buf),
        "%s failed to allocate, align: %ld, length:%ld\n",
        __func__, align, length);
    LOG(ERROR) << buf;
    return -ENOMEM;
}

int dpdk_for_uct_free(void *address, size_t length)
{
    char buf[128];
    snprintf(buf, sizeof(buf), "%s, address: %p, length:%ld\n",
             __func__, address, length);
    LOG(INFO) << buf;
    rte_free(address);
    //spdk_free(address);
    return 0;
}

void dpdk_init(int argc, char **argv)
{
#if 0
    char *eal_argv[] = {argv[0], (char *)"--in-memory", NULL};

    if (rte_eal_init(2, eal_argv) == -1) {
        errx(1, "rte_eal_init: %s", rte_strerror(rte_errno));
    }

    /* !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
     * Following is important. Above, we didn't specify dpdk runs on every cpu,
     * thus dpdk will default bind our thread to first cpu, and the cpu mask
     * is inherited by pthread_create(), causes every thread in future bind to
     * same cpu! This causes big performance problem!
     * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
     */
    unaffinitize_thread();
#endif

    // make iobuf use dpdk malloc & free
    butil::iobuf::set_blockmem_allocate_and_deallocate(dpdk_mem_allocate, 
        	dpdk_mem_free);
    // make ucx use dpdk malloc & free
    uct_set_user_mem_func(dpdk_for_uct_alloc, dpdk_for_uct_free); 
}

#endif

int main(int argc, char* argv[]) {
    std::string help_str = "dummy help infomation";
    GFLAGS_NS::SetUsageMessage(help_str);

    // Parse gflags. We recommend you to use gflags as well.
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_h) {
        fprintf(stderr, "%s\n%s\n%s", help_str.c_str(), help_str.c_str(), help_str.c_str());
        return 0;
    }

    set_pfs_options();

#if BRPC_WITH_DPDK
    if (pfs_spdk_setup())
        return 1;

    if (FLAGS_use_dpdk_malloc)
        dpdk_init(argc, argv);

    set_ucp_callback();

    if (pfsd_start(1))
        return 1;

#endif

    // Generally you only need one Server.
    brpc::Server server;

    // Instance of your service.
    example::EchoServiceImpl echo_service_impl;

    // Add the service into server. Notice the second parameter, because the
    // service is put on stack, we don't want server to delete it, otherwise
    // use brpc::SERVER_OWNS_SERVICE.
    if (server.AddService(&echo_service_impl, 
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }

    // Start the server. 
    brpc::ServerOptions options;
    options.mutable_ssl_options()->default_cert.certificate = "cert.pem";
    options.mutable_ssl_options()->default_cert.private_key = "key.pem";
    options.idle_timeout_sec = FLAGS_idle_timeout_s;
    options.max_concurrency = FLAGS_max_concurrency;
    options.internal_port = FLAGS_internal_port;
    options.enable_ucp = FLAGS_enable_ucp;
    options.ucp_address = FLAGS_ucp_address;
    options.ucp_port = FLAGS_ucp_port;
    if (server.Start(FLAGS_port, &options) != 0) {
        LOG(ERROR) << "Fail to start EchoServer";
        return -1;
    }

    // Wait until Ctrl-C is pressed, then Stop() and Join() the server.
    server.RunUntilAskedToQuit();

#if BRPC_WITH_DPDK
    pfsd_stop();
    pfsd_wait_stop();
    pfs_spdk_cleanup();
#endif
    return 0;
}
