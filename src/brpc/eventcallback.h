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
