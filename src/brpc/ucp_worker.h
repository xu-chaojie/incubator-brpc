// Copyright (c) 2022 Netease Inc.
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

// Authors: Xu Yifeng @ netease

#ifndef BRPC_UCP_WORKER_H
#define BRPC_UCP_WORKER_H

#include "butil/macros.h"
#include "butil/endpoint.h"
#include "bthread/bthread.h"
#include "brpc/eventcallback.h"
#include "brpc/ucp_connection.h"
#include <ucp/api/ucp.h> 
#include <list>
#include <map>
#include <queue>

namespace brpc {

class UcpWorkerPool;

class UcpWorker {
public:
    enum Status {
        UNINITIALIZED = 0,
        READY = 1,
        RUNNING = 2,
        STOPPING = 3,
    };

    UcpWorker(UcpWorkerPool *pool, int id);
    ~UcpWorker();

    int Start();
    void Stop();
    void Join();
    Status status() const { return status_; }
    void Wakeup();
    void MaybeWakeup();
    void DispatchExternalEvent(EventCallbackRef e);
    bthread_t Owner() const { return worker_tid_; }

private:
    DISALLOW_COPY_AND_ASSIGN(UcpWorker);

    int Accept(UcpConnection *conn, ucp_conn_request_h req);
    int Connect(UcpConnection *conn,  const butil::EndPoint &peer);
    int CreateUcpEp(UcpConnection *conn, ucp_conn_request_h req);

    static void* RunWorker(void *arg);
    static void *RunEventThread(void *arg);
    static void ErrorCallback(void *arg, ucp_ep_h, ucs_status_t);
    static void TagRecvCb(void *request, ucs_status_t status,
        const ucp_tag_recv_info_t *info, void *user_data);
    static void StreamRecvCb(void *request, ucs_status_t status, size_t length,
                           void *user_data);
    static void StreamSendCb(void *request, ucs_status_t status,
                           void *user_data);

    int  Initialize();
    void InvokeExternalEvents();
    void DoRunWorker();
    void DoRunEventLoop();
    void Release(UcpConnectionRef ref);
    void CheckExitingEp();
    ssize_t StartRecv(UcpConnection *conn);
    ssize_t StartSend(UcpConnection *conn, butil::IOBuf *buf[], int ndata);
    ssize_t StartSendInternal(UcpConnection *conn, butil::IOBuf *buf[], int ndata);
    void DispatchDataReady();
    void SetDataReady(const UcpConnectionRef & conn);
    void SetDataReadyLocked(const UcpConnectionRef & conn);
private:
    bthread::Mutex mutex_;
    bthread::Mutex external_mutex_;
    std::list<EventCallbackRef> external_events_;
    Status status_;
    // Worker id
    int id_;
    std::map<ucp_ep_h, UcpConnectionRef> conn_map_;
    struct ExitingEp {
        int magic;
        ucp_ep_h ep;
        ucs_status_ptr_t req;
        UcpConnectionRef conn;

        ExitingEp() {
            magic = 1234;
            ep = NULL;
            req = NULL;
        }
    };
    typedef std::list<ExitingEp> exiting_ep_list_t;
    exiting_ep_list_t exiting_ep_;
    // Acceptor bthread
    bthread_t worker_tid_;
    // Ucp event fd
    int event_fd_;
    // Ucp worker for acceptor
    ucp_worker_h ucp_worker_;

    std::queue<UcpConnectionRef> data_ready_;
    friend class UcpConnection;
};

} // namespace brpc

#endif // BRPC_UCP_WORKER_H
