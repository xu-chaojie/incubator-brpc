// Copyright (c) 2022 Netease.
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

#include "butil/logging.h"
#include "bthread/unstable.h"
#include "ucs/sys/sock.h"

#include "brpc/ucp_ctx.h"
#include "brpc/ucp_worker.h"

namespace brpc {

UcpWorker::UcpWorker(UcpWorkerPool *pool, int id)
    : status_(UNINITIALIZED)
    , id_(id)
    , worker_tid_(INVALID_BTHREAD)
    , event_fd_(-1)
    , ucp_worker_(NULL)
{
}

UcpWorker::~UcpWorker()
{
    Stop();
    Join();
}

int UcpWorker::Initialize()
{
    return 0;
}

int UcpWorker::Start()
{
    ucp_worker_h w;
    ucs_status_t stat;
    int efd = -1;

    BAIDU_SCOPED_LOCK(mutex_);
    if (status_ == UNINITIALIZED) {
        if (Initialize() != 0) {
            LOG(FATAL) << "Fail to initialize UcpWorker";
            return -1;
        }
        status_ = READY;
    }
    if (status_ != READY) {
        LOG(FATAL) << "UcpWorker hasn't stopped yet: status=" << status();
        return -1;
    }

    // Create ucp worker
    if (create_ucp_worker(&w, &efd, 0))
        return -1;

    // Need to prepare ucp_worker before register with brpc fd_wait
    stat = ucp_worker_arm(w);
    if (stat != UCS_OK) {
        LOG(ERROR) << "ucx_worker_arm failed ("
                   << ucs_status_string(stat) << ")";
        ucp_worker_destroy(w);
        return -1;
    }

    event_fd_ = efd;
    ucp_worker_ = w;

    if (bthread_start_background(&worker_tid_, NULL,
                                 RunWorker, this) != 0) {
        LOG(FATAL) << "Fail to start bthread";
        event_fd_ = -1;
        ucp_worker_destroy(w);
        ucp_worker_ = NULL;
        return -1;
    }

    status_ = RUNNING;
    return 0;
}

void UcpWorker::Stop()
{
    BAIDU_SCOPED_LOCK(mutex_);
    if (status_ != RUNNING) {
        return;
    }
    status_ = STOPPING;

    if (ucp_worker_) {
        ucs_status_t stat = ucp_worker_signal(ucp_worker_);
        if (stat != UCS_OK) {
            LOG(ERROR) << "ucp_worker_signal error ("
                       << ucs_status_string(stat) << ")";
        }
    }
}

void UcpWorker::Join()
{
    std::unique_lock<bthread::Mutex> mu(mutex_);
    if (status_ != STOPPING) {
        return;
    }
    mu.unlock();

    bthread_join(worker_tid_, NULL);

    ucp_worker_destroy(ucp_worker_);
    ucp_worker_ = NULL;

    {
        BAIDU_SCOPED_LOCK(mutex_);
        status_ = READY;
    }
}

void UcpWorker::Wakeup()
{
    ucs_status_t stat = ucp_worker_signal(ucp_worker_);
    if (stat != UCS_OK) {
        LOG(ERROR) << "ucp_worker_signal error ("
                   << ucs_status_string(stat) << ")";
    }
}

void UcpWorker::MaybeWakeup()
{
    if (bthread_self() != worker_tid_)
        Wakeup();
}

void UcpWorker::DispatchExternalEvent(EventCallbackRef e)
{
    external_mutex_.lock();
    external_events_.push_back(e);
    external_mutex_.unlock();
    Wakeup();
}

void UcpWorker::InvokeExternalEvents()
{
    external_mutex_.lock();
    while (!external_events_.empty()) {
        {
            auto e = external_events_.front();
            external_events_.pop_front();
            external_mutex_.unlock();
            if (e)
                e->do_request(0);
            // e is destructed here
        }
        external_mutex_.lock();
    }
    external_mutex_.unlock();
}

void* UcpWorker::RunWorker(void *arg)
{
    UcpWorker *w = (UcpWorker *)arg;
    w->DoRunWorker();
    return 0;
}

void UcpWorker::DoRunWorker()
{
    mutex_.lock();
    while (status_ != STOPPING) {
        mutex_.unlock();
        bthread_fd_wait(event_fd_, EPOLLIN);
        mutex_.lock();
again:
        while (ucp_worker_progress(ucp_worker_)) {
            DispatchDataReady();
            CheckExitingEp();
        }
        ucs_status_t stat = ucp_worker_arm(ucp_worker_);
        if (stat == UCS_ERR_BUSY) /* some events are arrived already */
            goto again;
        CHECK(stat == UCS_OK) << "ucx_worker_arm failed ("
            << ucs_status_string(stat) << ")";

        mutex_.unlock();
        InvokeExternalEvents();
        mutex_.lock();
    }
    mutex_.unlock();
}

int UcpWorker::Accept(UcpConnection *conn, ucp_conn_request_h req)
{
    BAIDU_SCOPED_LOCK(mutex_);
    int ret = CreateUcpEp(conn, req);
    if (ret == 0) {
        auto it = conn_map_.find(conn->ep_);
        CHECK(it == conn_map_.end()) << "repeated ep ?";
        conn_map_[conn->ep_] = conn;
        MaybeWakeup();
    }
    return ret;
}

int UcpWorker::Connect(UcpConnection *conn, const butil::EndPoint &peer)
{
    std::unique_lock<bthread::Mutex> lg(mutex_);
    int ret = create_ucp_ep(ucp_worker_, peer, ErrorCallback, this, &conn->ep_);
    if (ret == 0) {
        auto it = conn_map_.find(conn->ep_);
        CHECK(it == conn_map_.end()) << "repeated ep ?";
        conn_map_[conn->ep_] = conn;
        conn->remote_side_ = peer;
        MaybeWakeup();
    }
    return ret;
}

void UcpWorker::ErrorCallback(void *arg, ucp_ep_h ep, ucs_status_t status)
{
    UcpWorker *w = static_cast<UcpWorker *>(arg);
    // assert(w->mutex_.is_locked());
    auto it = w->conn_map_.find(ep);
    if (it == w->conn_map_.end()) {
        LOG(ERROR) << "can not find ep in ErrorCallback, may be moved";
        return;
    }
    UcpConnectionRef conn = it->second;
    butil::EndPointStr str = endpoint2str(conn->remote_side_);
    LOG(ERROR) << "Error occurred on ep " << ep << "," << str.c_str()
               << ", with status " << status
               << "(" << ucs_status_string(status) << ")";
    conn->ucp_code_.store(status ? status : UCS_ERR_IO_ERROR);
    w->SetDataReadyLocked(conn);
}

// Release the connection object indexed by ep, and gracefully disconnect it
void UcpWorker::Release(UcpConnectionRef conn)
{
    ucs_status_ptr_t request;
    ucp_ep_h ep = conn->ep_;
    BAIDU_SCOPED_LOCK(mutex_);

    auto it = conn_map_.find(ep);
    if (it != conn_map_.end()) {
        CHECK(conn == it->second);
        conn_map_.erase(it);
    }

    request = ucp_ep_close_nb(ep, UCP_EP_CLOSE_MODE_FLUSH);
    if (request == NULL) {
        conn->ep_ = NULL;
        DLOG(INFO) << "closed ep " << ep;
        return;
    } else if (UCS_PTR_IS_ERR(request)) {
        conn->ep_ = NULL;
        LOG(ERROR) << "ucp_ep_close_nb(" << ep << ") failed with status ("
                   << ucs_status_string(UCS_PTR_STATUS(request)) << ")";
        return;
    }
    // Remember it, we will check it later
    ExitingEp e;
    e.ep = ep;
    e.req = request;
    e.conn = conn;
    exiting_ep_.push_back(e);
    MaybeWakeup();
}

void UcpWorker::CheckExitingEp()
{
    // assert(mutex_.is_locked_by_me());
    exiting_ep_list_t::iterator it, next;
    for (it = exiting_ep_.begin(); it != exiting_ep_.end(); it = next) {
        CHECK(it->magic == 1234) << "magic bad";
        next = it;
        next++;
        ucs_status_t status;
        ucp_ep_h ep = it->ep;
        ucs_status_ptr_t request = it->req;
        UcpConnectionRef conn = it->conn;
        status = ucp_request_check_status(request);
        if (status == UCS_INPROGRESS)
            continue;
        DLOG(INFO) << "closed ep " << ep;
        ucp_request_release(request);
        conn->ep_ = NULL;
        next = exiting_ep_.erase(it);
    }
}

int UcpWorker::CreateUcpEp(UcpConnection *conn, ucp_conn_request_h req)
{
    int rc = create_ucp_ep(ucp_worker_, req, ErrorCallback, this, &conn->ep_);
    if (rc == 0) {
        ucp_ep_attr_t attr;
        attr.field_mask = UCP_EP_ATTR_FIELD_REMOTE_SOCKADDR;
        ucs_status_t st = ucp_ep_query(conn->ep_, &attr);
        if (st == UCS_OK) {
            conn->remote_side_ =
                butil::EndPoint(*(sockaddr_in*)&attr.remote_sockaddr,
                                butil::EndPoint::UCP);
        }
    }
    return rc;
}

void UcpWorker::StreamRecvCb(void *request, ucs_status_t status, size_t length,
                             void *user_data)
{
    // reference is hand-off to conn
    UcpConnectionRef conn((UcpConnection *)user_data, false);

    // Copy request handle
    auto req_h = conn->recv_req_;

    {
        // Lock iobuf mutex
        std::unique_lock<bthread::Mutex> lg_io(conn->io_mutex_);
 
        // Clear ucp request handle
        conn->recv_req_ = NULL;
    
        if (status != UCS_OK) {
            conn->ucp_code_.store(status);
        } else {
            conn->in_buf_.append_from_buffer(length);
        }
    }

    conn->worker_->SetDataReadyLocked(conn);
    ucp_request_free(req_h);
}

void UcpWorker::StreamSendCb(void *request, ucs_status_t status, void *user_data)
{
    // reference is hand-off to conn
    UcpConnectionRef conn((UcpConnection *)user_data, false);

    // lock iobuf mutex
    std::unique_lock<bthread::Mutex> lg_io(conn->io_mutex_);
    // free ucp request
    ucp_request_free(conn->send_req_);
    conn->send_req_ = NULL;

    if (status != UCS_OK) {
        conn->ucp_code_.store(status);
        conn->worker_->SetDataReadyLocked(conn);
    } else {
        conn->out_buf_.pop_front(conn->send_nbytes_);
        conn->worker_->StartSendInternal(conn.get(), NULL, 0);
    }
}

ssize_t UcpWorker::StartRecv(UcpConnection *conn)
{
    ucp_request_param_t param;
    void *msg;
    size_t total = 0, msg_length;

    std::unique_lock<bthread::Mutex> lg(mutex_);
    std::unique_lock<bthread::Mutex> lg_io(conn->io_mutex_);

    if (conn->recv_req_) {
        return 0;
    }

again:
    conn->recv_nbytes_ =
        conn->in_buf_.prepare_buffer(MAX_UCP_IO_BYTES,
            MAX_UCP_IOV, conn->recv_iov_, &conn->recv_nvec_);
    
    msg         = (conn->recv_nvec_ == 1) ? conn->recv_iov_[0].buffer : conn->recv_iov_;
    msg_length  = (conn->recv_nvec_ == 1) ? conn->recv_iov_[0].length : conn->recv_nvec_;

    param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                         UCP_OP_ATTR_FIELD_DATATYPE |
                         UCP_OP_ATTR_FIELD_USER_DATA;
    param.datatype = (conn->recv_nvec_ == 1) ? ucp_dt_make_contig(1) :
                         ucp_dt_make_iov();
    param.user_data = conn;
    conn->get(); // get reference;
    param.cb.recv_stream = StreamRecvCb;
    conn->recv_req_ = ucp_stream_recv_nbx(conn->ep_, msg, msg_length, &msg_length,
                            &param);
    if (conn->recv_req_ == NULL) {
        conn->in_buf_.append_from_buffer(msg_length);
        conn->put();
        total += msg_length;
        goto again;
    } else if (UCS_PTR_IS_ERR(conn->recv_req_)) {
        ucs_status_t st = UCS_PTR_STATUS(conn->recv_req_);
        conn->recv_req_ = NULL;
        conn->ucp_code_.store(st);
        lg_io.unlock();
        lg.unlock();
        conn->put();
        LOG(INFO) << "Failed to call ucp_stream_recv_nbx (" << ucs_status_string(st) << ")";
        if (total)
            return total;
        return -1;
    }

    return total;
}

void UcpWorker::DispatchDataReady()
{
    std::queue<UcpConnectionRef> tmp;

    tmp.swap(data_ready_);
    mutex_.unlock();
    while (!tmp.empty()) {
        auto conn = tmp.front();
        tmp.pop();
        conn->DataReady();
    }
    mutex_.lock();
}

void UcpWorker::SetDataReady(const UcpConnectionRef& conn)
{
    std::unique_lock<bthread::Mutex> lg(mutex_);

    if (!conn->data_ready_flag_) {
        data_ready_.push(conn);
        conn->data_ready_flag_ = true;
        MaybeWakeup();
    }
}

void UcpWorker::SetDataReadyLocked(const UcpConnectionRef &conn)
{
    data_ready_.push(conn);
    MaybeWakeup();
}

ssize_t UcpWorker::StartSend(UcpConnection *conn,
    butil::IOBuf *data_list[], int ndata)
{
    std::unique_lock<bthread::Mutex> lg(mutex_);
    std::unique_lock<bthread::Mutex> lg_io(conn->io_mutex_);

    return StartSendInternal(conn, data_list, ndata);
}

ssize_t UcpWorker::StartSendInternal(UcpConnection *conn,
    butil::IOBuf *data_list[], int ndata)
{
    ucp_request_param_t param;
    void *msg;
    size_t msg_length;
    size_t len = 0;

    // assert mutex_.is_locked_by_me()
    // assert conn->io_mutex_.is_locked_by_me()
    if (data_list != NULL) {
        for (int i = 0; i < ndata; ++i) {
            len += data_list[i]->length();
            conn->out_buf_.append(butil::IOBuf::Movable(*(data_list[i])));
        }
    }

    if (conn->send_req_) {
        return len;
    }

again:
    if (conn->ucp_code_.load()) {
        return len;
    }

    if (conn->out_buf_.empty()) {
        return len;
    }

    conn->send_nbytes_ = conn->out_buf_.fill_ucp_dt_iov(conn->send_iov_, MAX_UCP_IOV,
                            &conn->send_nvec_, 1024 * 1024);

    msg         = (conn->send_nvec_ == 1) ? conn->send_iov_[0].buffer : conn->send_iov_;
    msg_length  = (conn->send_nvec_ == 1) ? conn->send_iov_[0].length : conn->send_nvec_;

    memset(&param, 0, sizeof(param));
    param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                         UCP_OP_ATTR_FIELD_DATATYPE |
                         UCP_OP_ATTR_FIELD_USER_DATA;
    param.datatype = (conn->send_nvec_ == 1) ? ucp_dt_make_contig(1) :
                         ucp_dt_make_iov();
    param.user_data = conn;
    conn->get(); // get reference;
    param.cb.send = StreamSendCb;
    conn->send_req_ = ucp_stream_send_nbx(conn->ep_, msg, msg_length, &param);
    if (conn->send_req_ == NULL) {
        conn->out_buf_.pop_front(conn->send_nbytes_);
        conn->put();
        goto again;
    } else if (UCS_PTR_IS_ERR(conn->send_req_)) {
        ucs_status_t st = UCS_PTR_STATUS(conn->send_req_);
        conn->send_req_ = NULL;
        conn->ucp_code_.store(st);
        SetDataReadyLocked(conn);
        conn->put();
        LOG(ERROR) << "Failed to ucp_stream_send_nbx("
                   << ucs_status_string(st) << ")";
    }
    return len;
}

} // namespace brpc
