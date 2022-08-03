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
#include <sys/queue.h>
#include <assert.h>
#include <vector>

namespace brpc {

class UcpCm;
class UcpWorker;
class UcpConnection;

typedef butil::intrusive_ptr<UcpConnection> UcpConnectionRef;

struct MsgHeader {
    uint64_t sn;
};

struct UcpAmMsg;
struct UcpAmSendInfo;

typedef TAILQ_HEAD(UcpAmList, UcpAmMsg) UcpAmList;
typedef TAILQ_HEAD(UcpAmSendList, UcpAmSendInfo) UcpAmSendList;

// UcpAmMsg flags
enum {
    AMF_MSG_Q,
    AMF_RECV_Q,
    AMF_COMP_Q,
    AMF_RNDV,
    AMF_FINISH,
};

struct UcpAmMsg {
    TAILQ_ENTRY(UcpAmMsg) link;
    TAILQ_ENTRY(UcpAmMsg) comp_link;
    UcpConnectionRef conn;
    uint64_t sn;
    union {
        void *desc;
        void *data;
    };
    uint64_t length;
    butil::IOPortal buf;
    butil::iobuf_ucp_iov_t iov;
    int nvec;
    int flags;
    ucs_status_t code;
    void *req;

    void set_flag(int f) { flags |= UCS_BIT(f); }
    void clear_flag(int f) { flags &= ~UCS_BIT(f); }
    bool has_flag(int f) const { return !!(flags & UCS_BIT(f)); }

    UcpAmMsg();
};

struct UcpAmSendInfo {
    TAILQ_ENTRY(UcpAmSendInfo) link;
    UcpConnectionRef conn;
    butil::IOPortal buf;
    butil::iobuf_ucp_iov_t iov;
    MsgHeader header;
    ucs_status_t code;
    void *req;
    int nvec;

    UcpAmSendInfo();
};

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
    UcpAmList recv_q_;
    UcpAmSendList send_q_;
    uint64_t next_send_sn_;
    friend class UcpCm;
    friend class UcpWorker;
};

} // namespace brpc
#endif
