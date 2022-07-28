#ifndef BRPC_EVENTCALLBACK_H
#define BRPC_EVENTCALLBACK_H

#include <memory>

namespace brpc {
class EventCallback {
public:
    virtual void do_request(int fd_or_id) = 0;
    virtual ~EventCallback() {}       // we want a virtual destructor!!!
};

typedef std::shared_ptr<EventCallback> EventCallbackRef;
} // namespace brpc
#endif
