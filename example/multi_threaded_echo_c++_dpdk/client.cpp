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

// A client sending requests to server by multiple threads.

#include <gflags/gflags.h>
#include <bthread/bthread.h>
#include <butil/logging.h>
#include <brpc/server.h>
#include <brpc/channel.h>
#include "echo.pb.h"
#include <bvar/bvar.h>
#include <uct/api/uct.h>
#include <brpc/ucp_cm.h>

#include <rte_eal.h>
#include <rte_errno.h>
#include <rte_thread.h>
#include <rte_malloc.h>

#include <err.h>

DEFINE_int32(thread_num, 50, "Number of threads to send requests");
DEFINE_bool(use_bthread, false, "Use bthread to send requests");
DEFINE_int32(attachment_size, 0, "Carry so many byte attachment along with requests");
DEFINE_int32(request_size, 16, "Bytes of each request");
DEFINE_string(protocol, "baidu_std", "Protocol type. Defined in src/brpc/options.proto");
DEFINE_string(connection_type, "", "Connection type. Available values: single, pooled, short");
DEFINE_string(server, "0.0.0.0:8002", "IP Address of server");
DEFINE_string(load_balancer, "", "The algorithm for load balancing");
DEFINE_int32(timeout_ms, 1000, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)"); 
DEFINE_bool(dont_fail, false, "Print fatal when some call failed");
DEFINE_bool(enable_ssl, false, "Use SSL connection");
DEFINE_int32(dummy_port, -1, "Launch dummy server at this port");
DEFINE_bool(use_ucp, false, "Use ucp connection");
DEFINE_bool(verify_attachment, false, "Verify responsed attachment");

std::string g_request;
std::string g_attachment;

bvar::LatencyRecorder g_latency_recorder("client");
bvar::Adder<int> g_error_count("client_error_count");

static void* sender(void* arg) {
    // Normally, you should not call a Channel directly, but instead construct
    // a stub Service wrapping it. stub can be shared by all threads as well.
    example::EchoService_Stub stub(static_cast<google::protobuf::RpcChannel*>(arg));

    int log_id = 0;
    while (!brpc::IsAskedToQuit()) {
        // We will receive response synchronously, safe to put variables
        // on stack.
        example::EchoRequest request;
        example::EchoResponse response;
        brpc::Controller cntl;

        request.set_message(g_request);
        cntl.set_log_id(log_id++);  // set by user
        // Set attachment which is wired to network directly instead of 
        // being serialized into protobuf messages.
        cntl.request_attachment().append(g_attachment);

        // Because `done'(last parameter) is NULL, this function waits until
        // the response comes back or error occurs(including timedout).
        stub.Echo(&cntl, &request, &response, NULL);
        if (!cntl.Failed()) {
            if (FLAGS_verify_attachment) {
                if (g_attachment != cntl.response_attachment()) {
                    LOG(ERROR) << "attachment not same";
                    abort();
                }
            }
            g_latency_recorder << cntl.latency_us();
        } else {
            g_error_count << 1; 
            CHECK(brpc::IsAskedToQuit() || !FLAGS_dont_fail)
                << "error=" << cntl.ErrorText() << " latency=" << cntl.latency_us();
            // We can't connect to the server, sleep a while. Notice that this
            // is a specific sleeping to prevent this thread from spinning too
            // fast. You should continue the business logic in a production 
            // server rather than sleeping.
            bthread_usleep(50000);
        }
    }
    return NULL;
}

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
    //spdk_free(address);
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
    // Parse gflags. We recommend you to use gflags as well.
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);

    dpdk_init(argc, argv);

    // A Channel represents a communication line to a Server. Notice that 
    // Channel is thread-safe and can be shared by all threads in your program.
    brpc::Channel channel;
    
    // Initialize the channel, NULL means using default options.
    brpc::ChannelOptions options;
    if (FLAGS_enable_ssl) {
        options.mutable_ssl_options();
    }
    options.protocol = FLAGS_protocol;
    options.connection_type = FLAGS_connection_type;
    options.connect_timeout_ms = std::min(FLAGS_timeout_ms / 2, 100);
    options.timeout_ms = FLAGS_timeout_ms;
    options.max_retry = FLAGS_max_retry;
    options.use_ucp = FLAGS_use_ucp;
    if (channel.Init(FLAGS_server.c_str(), FLAGS_load_balancer.c_str(), &options) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
        return -1;
    }

    if (FLAGS_attachment_size > 0) {
        g_attachment.resize(FLAGS_attachment_size, 'a');
    }
    if (FLAGS_request_size <= 0) {
        LOG(ERROR) << "Bad request_size=" << FLAGS_request_size;
        return -1;
    }
    g_request.resize(FLAGS_request_size, 'r');

    if (FLAGS_dummy_port >= 0) {
        brpc::StartDummyServerAt(FLAGS_dummy_port);
    }

    std::vector<bthread_t> bids;
    std::vector<pthread_t> pids;
    if (!FLAGS_use_bthread) {
        pids.resize(FLAGS_thread_num);
        for (int i = 0; i < FLAGS_thread_num; ++i) {
            if (pthread_create(&pids[i], NULL, sender, &channel) != 0) {
                LOG(ERROR) << "Fail to create pthread";
                return -1;
            }
        }
    } else {
        bids.resize(FLAGS_thread_num);
        for (int i = 0; i < FLAGS_thread_num; ++i) {
            if (bthread_start_background(
                    &bids[i], NULL, sender, &channel) != 0) {
                LOG(ERROR) << "Fail to create bthread";
                return -1;
            }
        }
    }

    while (!brpc::IsAskedToQuit()) {
        sleep(1);
        LOG(INFO) << "Sending EchoRequest at qps=" << g_latency_recorder.qps(1)
                  << " latency=" << g_latency_recorder.latency(1);
    }

    LOG(INFO) << "EchoClient is going to quit";
    for (int i = 0; i < FLAGS_thread_num; ++i) {
        if (!FLAGS_use_bthread) {
            pthread_join(pids[i], NULL);
        } else {
            bthread_join(bids[i], NULL);
        }
    }

    return 0;
}
