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

#ifndef BRPC_EVENTCALLBACK_H
#define BRPC_EVENTCALLBACK_H

#include <butil/refcountedobj.h>
#include <butil/intrusive_ptr.hpp>

namespace brpc {

class EventCallback : public butil::RefCountedObject {
public:
    virtual void do_request(int fd_or_id) = 0;
};

typedef butil::intrusive_ptr<EventCallback> EventCallbackRef;

} // namespace brpc
#endif
