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

#include "brpc/ucp_connection.h"
#include "brpc/ucp_worker.h"
#include "brpc/ucp_cm.h"
#include "brpc/socket.h"

#include <gflags/gflags.h>
#include <sys/socket.h>
#include <ucs/datastruct/list.h>

namespace brpc {

DEFINE_int32(ucp_receive_lowat_bytes, 16384, "receive buffer low water bytes");

bthread_attr_t ucp_consumer_thread_attr = BTHREAD_ATTR_NORMAL;

UcpAmMsg::UcpAmMsg()
{
    sn = -1;
    data = nullptr;
    length = 0;
    nvec = 0;
    code = UCS_OK;
    req = nullptr;
    flags = 0;
}

UcpAmMsg *UcpAmMsg::Allocate(void)
{
    butil::ResourceId<UcpAmMsg> id;
    auto o = butil::get_resource<UcpAmMsg>(&id);
    o->id = id;
    return o;
}

void UcpAmMsg::Release(UcpAmMsg *o)
{
    o->sn = -1;
    o->data = nullptr;
    o->length = 0;
    o->nvec = 0;
    o->code = UCS_OK;
    o->req = nullptr;
    o->flags = 0;
    o->buf.clear();
    butil::return_resource<UcpAmMsg>(o->id);
}

UcpAmSendInfo::UcpAmSendInfo()
{
    header.sn = -1;
    code = UCS_OK;
    req = nullptr;
    nvec = 0;
}

UcpAmSendInfo *UcpAmSendInfo::Allocate(void)
{
    butil::ResourceId<UcpAmSendInfo> id;
    auto o = butil::get_resource<UcpAmSendInfo>(&id);
    o->id = id;
    return o;
}

void UcpAmSendInfo::Release(UcpAmSendInfo *o)
{
    o->header.sn = -1;
    o->code = UCS_OK;
    o->req = nullptr;
    o->nvec = 0;
    o->buf.clear();
    butil::return_resource<UcpAmSendInfo>(o->id);
}

UcpConnection::UcpConnection(UcpCm *cm, UcpWorker *w)
    : cm_(cm)
    , worker_(w)
    , ep_(NULL)
    , ucp_code_(UCS_OK)
    , socket_id_(-1)
    , socket_id_set_(false)
    , state_(STATE_NONE)
    , data_ready_flag_(false)
{
    remote_side_.set_ucp();
    TAILQ_INIT(&recv_q_);
    TAILQ_INIT(&send_q_);
    next_send_sn_ = 0;
}

UcpConnection::~UcpConnection()
{
    if (ep_) {
        LOG(ERROR) << "ep_ should be NULL";
    }
}

int UcpConnection::Accept(ucp_conn_request_h req)
{
    BAIDU_SCOPED_LOCK(mutex_);
    int rc = worker_->Accept(this, req);
    if (rc == 0) {
        state_ = STATE_OPEN;
    }
    return rc;
}

int UcpConnection::Connect(const butil::EndPoint &peer)
{
    BAIDU_SCOPED_LOCK(mutex_);
    int rc = worker_->Connect(this, peer);
    if (rc == 0) {
        state_ = STATE_OPEN;
    }
    return rc;
}

void UcpConnection::Close()
{
    BAIDU_SCOPED_LOCK(mutex_);
    if (state_ != STATE_OPEN) {
        return;
    }
    state_ = STATE_CLOSED;
    worker_->Release(this);
}

SocketId UcpConnection::GetSocketId() const
{
    return socket_id_;
}  

void UcpConnection::SetSocketId(SocketId id)
{
    BAIDU_SCOPED_LOCK(mutex_);
    socket_id_ = id;
    socket_id_set_ = true;
    if (ucp_code_.load()) {
        worker_->SetDataReady(this);
    } else if (state_ != STATE_CLOSED) {
        worker_->StartRecv(this);
    }
}

// called by UcpWorker to notify brpc Socket when data is ready
void UcpConnection::DataReady()
{
    data_ready_flag_ = false;
    if (state_ == STATE_OPEN && socket_id_set_)
        Socket::StartInputEvent(socket_id_, EPOLLIN, ucp_consumer_thread_attr);
    else {
        if (state_ != STATE_OPEN) {
            LOG(WARNING) << "brpc::Socket is closed, "
                            "DataReady does not notify the socket";
        } else {
            LOG(WARNING) << "brpc::Socket id is not set, "
                            "DataReady does not notify the socket";
        }
    }
}

ssize_t UcpConnection::Read(butil::IOBuf *out, size_t size_hint)
{
    ssize_t rc;
    BAIDU_SCOPED_LOCK(mutex_);
 
    // Connection closed, return EOF
    if (state_ != STATE_OPEN) {
        LOG(ERROR) << "Read with closed state";
        return 0;
    }

    {
        // Try to read from input buffer
        BAIDU_SCOPED_LOCK(io_mutex_);
        rc = in_buf_.cutn(out, size_hint); 
    }

    if (rc == 0) {
        if (ucp_code_.load()) // IO error happened, return EOF
            return 0;
    }

    if (rc == 0) {
        rc = -1;
        errno = EAGAIN;
    }
 
    return rc;
}

ssize_t UcpConnection::Write(butil::IOBuf *buf)
{
    butil::IOBuf *data_list[1];
    data_list[0] = buf;

    return Write(data_list, 1);
}

ssize_t UcpConnection::Write(butil::IOBuf *data_list[], int ndata)
{
    std::unique_lock<bthread::Mutex> mu(mutex_);
    if (state_ != STATE_OPEN) {
        errno = ENOTCONN;
        return -1;
    }

    if (ucp_code_.load()) {
        errno = ECONNRESET;
        return -1;
    }

    ssize_t len = worker_->StartSend(this, data_list, ndata);
    if (ucp_code_.load()) {
        errno = ECONNRESET;
        return -1;
    }
    return len;
}

} // namespace brpc
