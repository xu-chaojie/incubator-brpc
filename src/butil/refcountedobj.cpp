#include "refcountedobj.h"
                                                                                
namespace butil {

void intrusive_ptr_add_ref(RefCountedObject *p)
{
    p->get();
}

void intrusive_ptr_release(RefCountedObject *p)
{
    p->put();
}
} // namespace butil
