32.20. Behavior in Threaded Programs  
---  
[Prev](libpq-ssl.md "32.19. SSL Support") | [Up](libpq.md "Chapter 32. libpq — C Library")| Chapter 32. libpq — C Library| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](libpq-build.md "32.21. Building libpq Programs")  
  
* * *

## 32.20. Behavior in Threaded Programs #

As of version 17, libpq is always reentrant and thread-safe. However, one restriction is that no two threads attempt to manipulate the same `PGconn` object at the same time. In particular, you cannot issue concurrent commands from different threads through the same connection object. (If you need to run concurrent commands, use multiple connections.) 

`PGresult` objects are normally read-only after creation, and so can be passed around freely between threads. However, if you use any of the `PGresult`-modifying functions described in [Section 32.12](libpq-misc.md "32.12. Miscellaneous Functions") or [Section 32.14](libpq-events.md "32.14. Event System"), it's up to you to avoid concurrent operations on the same `PGresult`, too. 

In earlier versions, libpq could be compiled with or without thread support, depending on compiler options. This function allows the querying of libpq's thread-safe status: 

`PQisthreadsafe` #
    

Returns the thread safety status of the libpq library. 
    
    
    int PQisthreadsafe();
    

Returns 1 if the libpq is thread-safe and 0 if it is not. Always returns 1 on version 17 and above. 

The deprecated functions [`PQrequestCancel`](libpq-cancel.md#LIBPQ-PQREQUESTCANCEL) and [`PQoidStatus`](libpq-exec.md#LIBPQ-PQOIDSTATUS) are not thread-safe and should not be used in multithread programs. [`PQrequestCancel`](libpq-cancel.md#LIBPQ-PQREQUESTCANCEL) can be replaced by [`PQcancelBlocking`](libpq-cancel.md#LIBPQ-PQCANCELBLOCKING). [`PQoidStatus`](libpq-exec.md#LIBPQ-PQOIDSTATUS) can be replaced by [`PQoidValue`](libpq-exec.md#LIBPQ-PQOIDVALUE). 

If you are using Kerberos inside your application (in addition to inside libpq), you will need to do locking around Kerberos calls because Kerberos functions are not thread-safe. See function `PQregisterThreadLock` in the libpq source code for a way to do cooperative locking between libpq and your application. 

* * *

[Prev](libpq-ssl.md "32.19. SSL Support") | [Up](libpq.md "Chapter 32. libpq — C Library")|  [Next](libpq-build.md "32.21. Building libpq Programs")  
---|---|---  
32.19. SSL Support | [Home](index.md "PostgreSQL 17.5 Documentation")|  32.21. Building libpq Programs
