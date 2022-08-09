// Copyright (c) 2022 Netease, Inc.
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

// Authors: Xu Yifeng

#ifndef BRPC_UCP_ACCEPTOR_H
#define BRPC_UCP_ACCEPTOR_H

#include "bthread/bthread.h"
#include "bthread/condition_variable.h"
#include "butil/containers/flat_map.h"
#include "brpc/input_messenger.h"
#include <mutex>
#include <queue>
#include <ucp/api/ucp.h> 

namespace brpc {

struct UcpConnectStatistics {
};

// Accept connections from a specific port and then
// process messages from which it reads
class UcpAcceptor : public InputMessenger {
public:
    typedef butil::FlatMap<SocketId, UcpConnectStatistics> SocketMap;

    enum Status {
        UNINITIALIZED = 0,
        READY = 1,
        RUNNING = 2,
        STOPPING = 3,
    };

public:
    explicit UcpAcceptor(bthread_keytable_pool_t* pool = NULL);
    ~UcpAcceptor();

    // [thread-safe]
    // Can be called multiple times if the last `StartAccept' has been
    // completely stopped by calling `StopAccept' and `Join'.
    // Connections that has no data transmission for `idle_timeout_sec'
    // will be closed automatically iff `idle_timeout_sec' > 0
    // Return 0 on success, -1 otherwise.
    int StartAccept(const butil::EndPoint &endpoint, int idle_timeout_sec);

    // [thread-safe] Stop accepting connections.
    // `closewait_ms' is not used anymore.
    void StopAccept(int /*closewait_ms*/);

    // Wait until all existing Sockets(defined in socket.h) are recycled.
    void Join();

    // Get number of existing connections.
    size_t ConnectionCount() const;

    // Clear `conn_list' and append all connections into it.
    void ListConnections(std::vector<SocketId>* conn_list);

    // Clear `conn_list' and append all most `max_copied' connections into it.
    void ListConnections(std::vector<SocketId>* conn_list, size_t max_copied);

    Status status() const { return status_; }

    virtual int CheckHealth(Socket*) override;
                                                                                
    // Called after revived.                                                    
    virtual void AfterRevived(Socket*) override;

private:
    static void* CloseIdleConnections(void* arg);
    
    // Initialize internal structure. 
    int Initialize();

    // Remove the accepted socket `sock' from inside
    virtual void BeforeRecycle(Socket* sock);

    // Accept connections.
    static void* AcceptConnections(void *_this);
    static void UcpConnCb(ucp_conn_request_h conn_request, void *arg);
    void AcceptorLoop();
    void FinishDeferredAccept();
    void DoAccept(ucp_conn_request_h conn_request);

    bthread_keytable_pool_t* keytable_pool_; // owned by Server
    Status status_;
    int idle_timeout_sec_;

    // Acceptor bthread
    bthread_t acceptor_tid_;
    bthread_t close_idle_tid_;

    bthread::Mutex map_mutex_;
    bthread::ConditionVariable empty_cond_;
    
    // The map containing all the accepted sockets
    SocketMap socket_map_;

    // Ucp event fd
    int event_fd_;

    // Ucp worker for acceptor
    ucp_worker_h ucp_worker_;

    // Ucp listener
    ucp_listener_h ucp_listener_;

    std::queue<ucp_conn_request_h> accept_q_;
};

} // namespace brpc

#endif // BRPC_UCP_ACCEPTOR_H
