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

#include <rte_eal.h>
#include <rte_errno.h>
#include <rte_thread.h>
#include <rte_malloc.h>

#include <err.h>

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
        if (FLAGS_echo_attachment) {
            cntl->response_attachment().append(cntl->request_attachment());
        }
    }
};
}  // namespace example

DEFINE_bool(h, false, "print help information");

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
    return rte_malloc("iobuf", sz, align);
}

void dpdk_mem_free(void* p, size_t sz)
{
    rte_free(p);
}

int dpdk_for_uct_alloc(void **address, size_t align,
                      size_t length, const char *name)
{
    char buf[128];
    void *p = rte_malloc(name, length, align);
    if (p) {
        *address = p;
#if 0
        snprintf(buf, sizeof(buf),
            "%s, sucess allocated: %p, align: %ld, length:%ld, name:%s\n", 
            __func__, p, align, length, name);
        LOG(INFO) << buf;
#endif
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
#if 0
    char buf[128];
    snprintf(buf, sizeof(buf), "%s, address: %p, length:%ld\n",
             __func__, address, length);
    LOG(INFO) << buf;
#endif
    rte_free(address);
    return 0;
}

void dpdk_init(int argc, char **argv)
{
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

    // make iobuf use dpdk malloc & free
    butil::iobuf::set_blockmem_allocate_and_deallocate(dpdk_mem_allocate, 
        	dpdk_mem_free);
    // make ucx use dpdk malloc & free
    uct_set_user_mem_func(dpdk_for_uct_alloc, dpdk_for_uct_free); 
}

int main(int argc, char* argv[]) {
    std::string help_str = "dummy help infomation";
    GFLAGS_NS::SetUsageMessage(help_str);

    // Parse gflags. We recommend you to use gflags as well.
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_h) {
        fprintf(stderr, "%s\n%s\n%s", help_str.c_str(), help_str.c_str(), help_str.c_str());
        return 0;
    }

    dpdk_init(argc, argv);

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
    return 0;
}
