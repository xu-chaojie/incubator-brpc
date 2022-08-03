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

#include <endian.h>
#include <limits.h>

#define AM_ID  0

namespace brpc {

UcpWorker::UcpWorker(UcpWorkerPool *pool, int id)
    : status_(UNINITIALIZED)
    , id_(id)
    , worker_tid_(INVALID_BTHREAD)
    , event_fd_(-1)
    , ucp_worker_(NULL)
    , free_data_(NULL)
    , free_data_count_(0)
{
    TAILQ_INIT(&msg_q_);
    TAILQ_INIT(&recv_comp_q_);
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

    event_fd_ = efd;
    ucp_worker_ = w;

    // Need to prepare ucp_worker before register with brpc fd_wait
    stat = ucp_worker_arm(w);
    if (stat != UCS_OK) {
        LOG(ERROR) << "ucx_worker_arm failed ("
                   << ucs_status_string(stat) << ")";
        goto destroy;
    }

    if (!SetAmCallback()) {
        goto destroy;
    }

    if (bthread_start_background(&worker_tid_, NULL,
                                 RunWorker, this) != 0) {
        LOG(FATAL) << "Fail to start bthread";
        goto destroy;
    }

    status_ = RUNNING;
    return 0;

destroy:
    ucp_worker_destroy(ucp_worker_);
    ucp_worker_ = NULL;
    event_fd_ = -1;
    return -1;
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

bool UcpWorker::SetAmCallback()
{
    ucp_am_handler_param_t  param;
    ucs_status_t            status;

    memset(&param, 0, sizeof(param));
    param.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID |
                       UCP_AM_HANDLER_PARAM_FIELD_CB |
                       UCP_AM_HANDLER_PARAM_FIELD_ARG|
                       UCP_AM_HANDLER_PARAM_FIELD_FLAGS;
    param.id         = AM_ID;
    param.cb         = AmCallback;
    param.arg        = this;
    param.flags      = UCP_AM_FLAG_WHOLE_MSG | UCP_AM_FLAG_PERSISTENT_DATA;
    status           = ucp_worker_set_am_recv_handler(ucp_worker_, &param);
    if (status != UCS_OK) {
        LOG(ERROR) << "can not set am handler";
        return false;
    }
    return true;
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
            DispatchAmMsgQ();
            DispatchRecvCompQ();
            DispatchDataReady();
            CheckExitingEp();
            RecycleFreeData();
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

ucs_status_t UcpWorker::AmCallback(void *arg,
     const void *header, size_t header_length, void *data, size_t length,
     const ucp_am_recv_param_t *param)
{
    UcpWorker *w = (UcpWorker *)arg;
    return w->DoAmCallback(header, header_length, data, length, param);
}

ucs_status_t UcpWorker::DoAmCallback(
     const void *header, size_t header_length, void *data, size_t length,
     const ucp_am_recv_param_t *param)
{
    // assert(mutex_.is_locked_by_me());
    MsgHeader *mh = (MsgHeader *)header;
    if (mh == NULL) {
        LOG(ERROR) << "header is NULL";
        return UCS_OK;
    } 
    if (header_length < sizeof(*mh)) {
        LOG(ERROR) << "header_length is less than " << sizeof(*mh);
        return UCS_OK;
    }
    if (!(param->recv_attr & UCP_AM_RECV_ATTR_FIELD_REPLY_EP)) {
        LOG(ERROR) << "UCP_AM_RECV_ATTR_FIELD_REPLY_EP not set";
        return UCS_OK;
    }
    if (!(param->recv_attr & (UCP_AM_RECV_ATTR_FLAG_DATA |
                              UCP_AM_RECV_ATTR_FLAG_RNDV))) {
        LOG(ERROR) << "neither UCP_AM_RECV_ATTR_FIELD_DATA nor UCP_AM_RECV_ATTR_FLAG_RNDV is set";
        return UCS_OK;
    }

    auto it = conn_map_.find(param->reply_ep);
    if (it == conn_map_.end()) {
        LOG(ERROR) << "can not find ep in conn_map_";
        return UCS_OK;
    }

    UcpConnectionRef& conn = it->second;
    UcpAmMsg *msg = new UcpAmMsg;
    if (msg == NULL) {
        LOG(ERROR) << "can not allocate UcpMsg";
        return UCS_OK;
    }
    msg->conn = conn;
    msg->sn = le64toh(mh->sn);
    msg->data = data;
    msg->length = length;
    if (param->recv_attr & UCP_AM_RECV_ATTR_FLAG_RNDV) {
        msg->set_flag(AMF_RNDV);
    }
    msg->set_flag(AMF_MSG_Q);
    TAILQ_INSERT_TAIL(&msg_q_, msg, link);

    return UCS_INPROGRESS;
}

void UcpWorker::ReleaseWorkerData(void *data, void *arg)
{
    UcpWorker *w = (UcpWorker *)arg;
    void **ptr = (void **)data;

    do {
        *ptr = w->free_data_.load(std::memory_order_relaxed);
    } while (!w->free_data_.compare_exchange_strong(*ptr, data,
             std::memory_order_relaxed));
    if (++w->free_data_count_ > 100) {
        w->free_data_count_ = 0;
        w->MaybeWakeup();
    }
}

void UcpWorker::DispatchAmMsgQ()
{
    UcpAmMsg *msg, *msg2;
    ucp_request_param_t param;
    void *buf;
    size_t len; 
    int rc;
    UcpAmList tmp_list;

    // assert(mutex_.is_locked_by_me());
    if (TAILQ_EMPTY(&msg_q_)) {
        return;
    }

    TAILQ_INIT(&tmp_list);
    TAILQ_CONCAT(&tmp_list, &msg_q_, link); // msg_q is inited by TAILQ_CONCAT
    mutex_.unlock();

    while (!TAILQ_EMPTY(&tmp_list)) {
        msg = TAILQ_FIRST(&tmp_list);
        CHECK(msg->has_flag(AMF_MSG_Q)) << "not on msg queue";
        TAILQ_REMOVE(&tmp_list, msg, link);
        msg->clear_flag(AMF_MSG_Q);
        UcpConnectionRef conn = msg->conn;
        if (!msg->has_flag(AMF_RNDV)) {
            if (msg->length >= 8) {
                msg->buf.append_user_data(msg->data, msg->length, ReleaseWorkerData, this);
            } else {
                msg->buf.append(msg->data, msg->length);
                ucp_am_data_release(ucp_worker_, msg->data);
            }
            msg->req = nullptr;
            mutex_.lock();
            goto request_done;
        }
 
        rc = msg->buf.prepare_buffer(msg->length, INT_MAX,
                &msg->iov, &msg->nvec);
        if (rc == -1) {
            LOG(FATAL) << "prepare failure";
        }

        buf = (msg->nvec == 1) ? msg->iov[0].buffer : (void *)&msg->iov[0];
        len = (msg->nvec == 1) ? msg->iov[0].length : msg->nvec;
        param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK  |
                             UCP_OP_ATTR_FIELD_DATATYPE  |
                             UCP_OP_ATTR_FIELD_USER_DATA;
        param.datatype = (msg->nvec == 1) ? ucp_dt_make_contig(1) :
                         ucp_dt_make_iov();
        param.cb.recv_am = AmRecvCallback;
        param.user_data = msg;
        mutex_.lock();
        msg->req = ucp_am_recv_data_nbx(ucp_worker_,
                        msg->desc, buf, len, &param);
request_done:
        CHECK(!msg->has_flag(AMF_RECV_Q)) << "already on receive queue";
        TAILQ_FOREACH_REVERSE(msg2, &conn->recv_q_, UcpAmList, link) {
            if (msg->sn > msg2->sn) {
                TAILQ_INSERT_AFTER(&conn->recv_q_, msg2, msg, link);
                msg->set_flag(AMF_RECV_Q);
                break;
            }
        }

        if (!msg->has_flag(AMF_RECV_Q)) {
            TAILQ_INSERT_HEAD(&conn->recv_q_, msg, link);
            msg->set_flag(AMF_RECV_Q);
        }

        if (msg->req == nullptr) {
            msg->code = UCS_OK;
            TAILQ_INSERT_TAIL(&recv_comp_q_, msg, comp_link);
            msg->set_flag(AMF_COMP_Q);
        } else if (UCS_PTR_IS_ERR(msg->req)) {
            ucs_status_t st = UCS_PTR_STATUS(msg->req);
            msg->code = st;
            TAILQ_INSERT_TAIL(&recv_comp_q_, msg, comp_link);
            msg->set_flag(AMF_COMP_Q);
            LOG(INFO) << "Failed to call ucp_am_recv_nbx (" << ucs_status_string(st) << ")";
        }
        mutex_.unlock();
    }

    mutex_.lock();
}

void UcpWorker::AmRecvCallback(void *request, ucs_status_t status,
    size_t length, void *user_data)
{
    UcpAmMsg *msg = (UcpAmMsg *)user_data;
    msg->conn->worker_->DoAmRecvCallback(msg, status, length);
}

void UcpWorker::DoAmRecvCallback(UcpAmMsg *msg, ucs_status_t status,
    size_t length)
{
    // assert(mutex_.is_locked_by_me());
    ucp_request_free(msg->req);
    msg->code = status;
    CHECK(msg->length == length) << "strange, length not equal, expect "
                                 << msg->length << ", actual " << length;
    CHECK(!msg->has_flag(AMF_COMP_Q)) << "already on comp queue";
    TAILQ_INSERT_TAIL(&recv_comp_q_, msg, comp_link);
    msg->set_flag(AMF_COMP_Q);
    if (status != UCS_OK) {
        LOG(ERROR ) << "receive with error ("
                    << ucs_status_string(status) << ")";
    }
}

void UcpWorker::DispatchRecvCompQ()
{
    UcpAmList tmp_list;
    // assert(mutex_.is_locked_by_me());

    if (TAILQ_EMPTY(&recv_comp_q_)) {
        return;
    }
    TAILQ_INIT(&tmp_list);
    TAILQ_CONCAT(&tmp_list, &recv_comp_q_, comp_link);
    mutex_.unlock();

    while (!TAILQ_EMPTY(&tmp_list)) {
        UcpAmMsg *msg = TAILQ_FIRST(&tmp_list);
        UcpConnectionRef conn = msg->conn;

        CHECK(!msg->has_flag(AMF_FINISH)) << "finish should not set";
        msg->set_flag(AMF_FINISH);

        CHECK(msg->has_flag(AMF_COMP_Q)) << "not on comp queue";
        TAILQ_REMOVE(&tmp_list, msg, comp_link);
        msg->clear_flag(AMF_COMP_Q);

        if (msg->code == UCS_OK) {
            if (msg->has_flag(AMF_RNDV))
                msg->buf.append_from_buffer(msg->length);
        }
        bool avail = CheckConnRecvQ(conn);
        if (avail) {
            SetDataReady(conn);
        }
    }

    mutex_.lock();
}

bool UcpWorker::CheckConnRecvQ(const UcpConnectionRef& conn)
{
    bool avail = false;

    conn->io_mutex_.lock();
    while (!TAILQ_EMPTY(&conn->recv_q_)) {
        UcpAmMsg *msg = TAILQ_FIRST(&conn->recv_q_);
        if (!msg->has_flag(AMF_FINISH))
            break;
        CHECK(msg->has_flag(AMF_RECV_Q)) << "not on recv queue";
        CHECK(!msg->has_flag(AMF_COMP_Q)) << "still on comp queue";
        if (msg->code == UCS_OK) {
            conn->in_buf_.append(butil::IOBuf::Movable(msg->buf));
        } else {
            conn->ucp_code_.store(msg->code);
        }
        avail = true;
        TAILQ_REMOVE(&conn->recv_q_, msg, link);
        msg->clear_flag(AMF_RECV_Q);
        delete msg;
    }
    conn->io_mutex_.unlock();
    return avail;
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

ssize_t UcpWorker::StartRecv(UcpConnection *conn)
{
    bool empty;

    conn->io_mutex_.lock();
    empty = conn->in_buf_.empty() && conn->ucp_code_.load() == 0;
    conn->io_mutex_.unlock();

    if (!empty)
        SetDataReady(conn);  
    return 0;
}

void UcpWorker::DispatchDataReady()
{
    std::queue<UcpConnectionRef> tmp;

    if (data_ready_.empty()) {
        return;
    }

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

    SetDataReadyLocked(conn);
}

void UcpWorker::SetDataReadyLocked(const UcpConnectionRef &conn)
{
    if (!conn->data_ready_flag_) {
        data_ready_.push(conn);
        conn->data_ready_flag_ = true;
        MaybeWakeup();
    }
}

void UcpWorker::AmSendCb(void *request, ucs_status_t status, void *user_data)
{
    UcpAmSendInfo *msg = (UcpAmSendInfo *)user_data;
    UcpConnectionRef conn = msg->conn;

    if (status != UCS_OK) {
        conn->ucp_code_.store(status);
        conn->worker_->SetDataReadyLocked(msg->conn);
    }
    
    TAILQ_REMOVE(&conn->send_q_, msg, link);
    ucp_request_free(msg->req);
    delete msg;
}

ssize_t UcpWorker::StartSend(UcpConnection *conn,
    butil::IOBuf *data_list[], int ndata)
{
    ucp_request_param_t param;
    void *buf = NULL;
    size_t total = 0;
    size_t len = 0;

    if (conn->ucp_code_.load()) {
        errno = EIO;
        return -1;
    }
    if (data_list == NULL || ndata == 0)
        return 0;

    for (int i = 0; i < ndata; ++i) {
        UcpAmSendInfo *msg = new UcpAmSendInfo;
        if (msg == NULL) {
            LOG(ERROR) << "cannot allocater UcpAmSendInfo";
            return total;
        }
        msg->conn = conn;
        msg->header.sn = htole64(conn->next_send_sn_);
        msg->buf.append(butil::IOBuf::Movable(*data_list[i]));
        msg->buf.fill_ucp_iov(&msg->iov, INT_MAX,
                              &msg->nvec, ULONG_MAX);
        buf = (msg->nvec == 1) ? msg->iov[0].buffer : (void *)&msg->iov[0];
        len = (msg->nvec == 1) ? msg->iov[0].length : msg->nvec;

        memset(&param, 0, sizeof(param));
        param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                             UCP_OP_ATTR_FIELD_DATATYPE |
                             UCP_OP_ATTR_FIELD_USER_DATA|
                             UCP_OP_ATTR_FIELD_FLAGS;
        param.flags = UCP_AM_SEND_FLAG_REPLY;
        param.datatype = (msg->nvec == 1) ? ucp_dt_make_contig(1) :
                         ucp_dt_make_iov();
        param.user_data = msg;
        param.cb.send = AmSendCb;
        mutex_.lock();
        TAILQ_INSERT_TAIL(&conn->send_q_, msg, link);
        msg->req = ucp_am_send_nbx(conn->ep_, AM_ID, &msg->header, sizeof(msg->header), buf, len, &param);
        if (msg->req == NULL) {
            TAILQ_REMOVE(&conn->send_q_, msg, link);
            total += msg->buf.length();
            mutex_.unlock();
            delete msg;
            conn->next_send_sn_++;
        } else if (UCS_PTR_IS_ERR(msg->req)) {
            TAILQ_REMOVE(&conn->send_q_, msg, link);
            ucs_status_t st = UCS_PTR_STATUS(msg->req);
            conn->ucp_code_.store(st);
            SetDataReadyLocked(conn);
            mutex_.unlock();
            delete msg;
            LOG(ERROR) << "Failed to ucp_am_send_nbx("
                       << ucs_status_string(st) << ")";
            errno = EIO;
            return total ? total : -1;
        } else {
            total += msg->buf.length();
            mutex_.unlock();
            conn->next_send_sn_++;
        }
    }
    return total;
}

void UcpWorker::RecycleFreeData()
{
    if (!free_data_.load(std::memory_order_relaxed))
        return;
    free_data_count_ = 0;
    void *p = free_data_.exchange(NULL, std::memory_order_release);
    while (p) {
        void *next = *(void **)p;
        ucp_am_data_release(ucp_worker_, p);
        p = next;
    }
}

} // namespace brpc
