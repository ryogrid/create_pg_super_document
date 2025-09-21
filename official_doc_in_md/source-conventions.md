54.4. Miscellaneous Coding Conventions  
---  
[Prev](error-style-guide.md "54.3. Error Message Style Guide") | [Up](source.md "Chapter 54. PostgreSQL Coding Conventions")| Chapter 54. PostgreSQL Coding Conventions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](nls.md "Chapter 55. Native Language Support")  
  
* * *

## 54.4. Miscellaneous Coding Conventions #

### C Standard #

Code in PostgreSQL should only rely on language features available in the C99 standard. That means a conforming C99 compiler has to be able to compile postgres, at least aside from a few platform dependent pieces. 

A few features included in the C99 standard are, at this time, not permitted to be used in core PostgreSQL code. This currently includes variable length arrays, intermingled declarations and code, `//` comments, universal character names. Reasons for that include portability and historical practices. 

Features from later revisions of the C standard or compiler specific features can be used, if a fallback is provided. 

For example `_Static_assert()` and `__builtin_constant_p` are currently used, even though they are from newer revisions of the C standard and a GCC extension respectively. If not available we respectively fall back to using a C99 compatible replacement that performs the same checks, but emits rather cryptic messages and do not use `__builtin_constant_p`. 

### Function-Like Macros and Inline Functions #

Both macros with arguments and `static inline` functions may be used. The latter are preferable if there are multiple-evaluation hazards when written as a macro, as e.g., the case with 
    
    
    #define Max(x, y)       ((x) > (y) ? (x) : (y))
    

or when the macro would be very long. In other cases it's only possible to use macros, or at least easier. For example because expressions of various types need to be passed to the macro. 

When the definition of an inline function references symbols (i.e., variables, functions) that are only available as part of the backend, the function may not be visible when included from frontend code. 
    
    
    #ifndef FRONTEND
    static inline MemoryContext
    MemoryContextSwitchTo(MemoryContext context)
    {
        MemoryContext old = CurrentMemoryContext;
    
        CurrentMemoryContext = context;
        return old;
    }
    #endif   /* FRONTEND */
    

In this example `CurrentMemoryContext`, which is only available in the backend, is referenced and the function thus hidden with a `#ifndef FRONTEND`. This rule exists because some compilers emit references to symbols contained in inline functions even if the function is not used. 

### Writing Signal Handlers #

To be suitable to run inside a signal handler code has to be written very carefully. The fundamental problem is that, unless blocked, a signal handler can interrupt code at any time. If code inside the signal handler uses the same state as code outside chaos may ensue. As an example consider what happens if a signal handler tries to acquire a lock that's already held in the interrupted code. 

Barring special arrangements code in signal handlers may only call async-signal safe functions (as defined in POSIX) and access variables of type `volatile sig_atomic_t`. A few functions in `postgres` are also deemed signal safe, importantly `SetLatch()`. 

In most cases signal handlers should do nothing more than note that a signal has arrived, and wake up code running outside of the handler using a latch. An example of such a handler is the following: 
    
    
    static void
    handle_sighup(SIGNAL_ARGS)
    {
        got_SIGHUP = true;
        SetLatch(MyLatch);
    }
    

### Calling Function Pointers #

For clarity, it is preferred to explicitly dereference a function pointer when calling the pointed-to function if the pointer is a simple variable, for example: 
    
    
    (*emit_log_hook) (edata);
    

(even though `emit_log_hook(edata)` would also work). When the function pointer is part of a structure, then the extra punctuation can and usually should be omitted, for example: 
    
    
    paramInfo->paramFetch(paramInfo, paramId);
    

* * *

[Prev](error-style-guide.md "54.3. Error Message Style Guide") | [Up](source.md "Chapter 54. PostgreSQL Coding Conventions")|  [Next](nls.md "Chapter 55. Native Language Support")  
---|---|---  
54.3. Error Message Style Guide | [Home](index.md "PostgreSQL 17.5 Documentation")|  Chapter 55. Native Language Support
