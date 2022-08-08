#ifndef REFCOUNTEDOBJ_H
#define REFCOUNTEDOBJ_H

#include <butil/logging.h>
#include <butil/atomicops.h>

#include <assert.h>

namespace butil {

struct RefCountedObject {
private:
    butil::atomic<int> nref_;
public:
    RefCountedObject(int n=0) : nref_(n) {}
    virtual ~RefCountedObject()
    {
        assert(nref_ == 0);
    }

    RefCountedObject *get()
    {
        nref_.fetch_add(1, std::memory_order_relaxed);
        return this;
    }

    void put()
    {
        int v = nref_.fetch_sub(1, std::memory_order_relaxed);
        if ((v - 1) == 0)
            delete this;
    }

    int get_nref() {
        return nref_.load(std::memory_order_relaxed);
    }
};

void intrusive_ptr_add_ref(RefCountedObject *p);
void intrusive_ptr_release(RefCountedObject *p);

} //namepace butil

#endif
