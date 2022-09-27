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

#ifndef UCP_CTX_H
#define UCP_CTX_H

#include <ucp/api/ucp.h>

#include "butil/macros.h"
#include "butil/endpoint.h"

namespace brpc {

class UCP_Context {
public:
    UCP_Context();
    ~UCP_Context();

    ucp_context_h context() const { return context_; }
    operator ucp_context_h() const { return context_; }

    int init();
    void fini();

private:
    DISALLOW_COPY_AND_ASSIGN(UCP_Context);

    ucp_context_h context_;
    int cpu_latency_fd_;
};

UCP_Context* get_or_new_ucp_ctx();
/* Simple wrappers for ucx */
int create_ucp_worker(ucp_context_h ucp_ctx, ucp_worker_h *ucp_worker,
    int event, const char *name, int *efd);
int create_ucp_ep(ucp_worker_h w, ucp_conn_request_h conn_request,
    ucp_err_handler_cb_t err_cb, void *err_arg, ucp_ep_h *ep);
int create_ucp_ep(ucp_worker_h w, const butil::EndPoint &peer,
    ucp_err_handler_cb_t err_cb, void *err_arg, ucp_ep_h *ep);

} // namespace brpc

#endif // UCP_CTX_H
