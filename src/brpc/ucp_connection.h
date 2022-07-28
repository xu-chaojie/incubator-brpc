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

// Authors: Xu Yifeng @ netease

#ifndef BRPC_UCP_CONNECTION_H
#define BRPC_UCP_CONNECTION_H

#include "butil/endpoint.h"
#include "butil/intrusive_ptr.hpp"
#include "butil/atomicops.h"
#include "butil/iobuf.h"
#include "bthread/mutex.h"
#include "bthread/condition_variable.h"
#include "brpc/RefCountedObj.h"
#include "brpc/socket_id.h"
#include "brpc/eventcallback.h"
#include <ucp/api/ucp.h>

namespace brpc {

class UcpCm;
class UcpWorker;
class UcpConnection;

typedef butil::intrusive_ptr<UcpConnection> UcpConnectionRef;

const int MAX_UCP_IOV = 16;
const int MAX_UCP_IO_BYTES = 65536;

class UcpConnection : public RefCountedObject {
public:
    UcpConnection(UcpCm* cm, UcpWorker *w);
    virtual ~UcpConnection();

    UcpCm *GetCm() const { return cm_; }
    UcpWorker *GetWorker() const { return worker_; }
    void SetSocketId(SocketId id);
    SocketId GetSocketId() const;
    ssize_t Read(butil::IOBuf *out, size_t n); 
    ssize_t Write(butil::IOBuf *buf); 
    ssize_t Write(butil::IOBuf *data_list[], int ndata);
private:
    void Close();
    int Accept(ucp_conn_request_h req);
    int Connect(const butil::EndPoint &peer);
    void DataReady();

private:
    enum {
        STATE_NONE,
        STATE_OPEN,
        STATE_CLOSED,
    };

    mutable bthread::Mutex mutex_;
    bthread::ConditionVariable cond_;
    UcpCm *cm_;
    UcpWorker *worker_;
    ucp_ep_h ep_;
    butil::atomic<ucs_status_t> ucp_code_;
    butil::atomic<SocketId> socket_id_;
    int socket_id_set_;
    int state_;
    // Cached remote address
    butil::EndPoint remote_side_;

    // Populated by UcpWorker
    bool data_ready_flag_;
    mutable bthread::Mutex io_mutex_;
    butil::IOPortal in_buf_;
    ucs_status_ptr_t recv_req_;
    int recv_nvec_;
    ssize_t recv_nbytes_;
    ucp_dt_iov_t recv_iov_[MAX_UCP_IOV];

    butil::IOBuf out_buf_;
    ucs_status_ptr_t send_req_;
    int send_nvec_;
    ssize_t send_nbytes_;
    ucp_dt_iov_t send_iov_[MAX_UCP_IOV];
    friend class UcpCm;
    friend class UcpWorker;
};

} // namespace brpc
#endif
