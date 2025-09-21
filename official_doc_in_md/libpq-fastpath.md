32.8. The Fast-Path Interface  
---  
[Prev](libpq-cancel.md "32.7. Canceling Queries in Progress") | [Up](libpq.md "Chapter 32. libpq — C Library")| Chapter 32. libpq — C Library| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](libpq-notify.md "32.9. Asynchronous Notification")  
  
* * *

## 32.8. The Fast-Path Interface #

PostgreSQL provides a fast-path interface to send simple function calls to the server. 

### Tip

This interface is somewhat obsolete, as one can achieve similar performance and greater functionality by setting up a prepared statement to define the function call. Then, executing the statement with binary transmission of parameters and results substitutes for a fast-path function call. 

The function `PQfn` requests execution of a server function via the fast-path interface: 
    
    
    PGresult *PQfn(PGconn *conn,
                   int fnid,
                   int *result_buf,
                   int *result_len,
                   int result_is_int,
                   const PQArgBlock *args,
                   int nargs);
    
    typedef struct
    {
        int len;
        int isint;
        union
        {
            int *ptr;
            int integer;
        } u;
    } PQArgBlock;
    

The _`fnid`_ argument is the OID of the function to be executed. _`args`_ and _`nargs`_ define the parameters to be passed to the function; they must match the declared function argument list. When the _`isint`_ field of a parameter structure is true, the _`u.integer`_ value is sent to the server as an integer of the indicated length (this must be 2 or 4 bytes); proper byte-swapping occurs. When _`isint`_ is false, the indicated number of bytes at _`*u.ptr`_ are sent with no processing; the data must be in the format expected by the server for binary transmission of the function's argument data type. (The declaration of _`u.ptr`_ as being of type `int *` is historical; it would be better to consider it `void *`.) _`result_buf`_ points to the buffer in which to place the function's return value. The caller must have allocated sufficient space to store the return value. (There is no check!) The actual result length in bytes will be returned in the integer pointed to by _`result_len`_. If a 2- or 4-byte integer result is expected, set _`result_is_int`_ to 1, otherwise set it to 0. Setting _`result_is_int`_ to 1 causes libpq to byte-swap the value if necessary, so that it is delivered as a proper `int` value for the client machine; note that a 4-byte integer is delivered into _`*result_buf`_ for either allowed result size. When _`result_is_int`_ is 0, the binary-format byte string sent by the server is returned unmodified. (In this case it's better to consider _`result_buf`_ as being of type `void *`.) 

`PQfn` always returns a valid `PGresult` pointer, with status `PGRES_COMMAND_OK` for success or `PGRES_FATAL_ERROR` if some problem was encountered. The result status should be checked before the result is used. The caller is responsible for freeing the `PGresult` with [`PQclear`](libpq-exec.md#LIBPQ-PQCLEAR) when it is no longer needed. 

To pass a NULL argument to the function, set the _`len`_ field of that parameter structure to `-1`; the _`isint`_ and _`u`_ fields are then irrelevant. 

If the function returns NULL, _`*result_len`_ is set to `-1`, and _`*result_buf`_ is not modified. 

Note that it is not possible to handle set-valued results when using this interface. Also, the function must be a plain function, not an aggregate, window function, or procedure. 

* * *

[Prev](libpq-cancel.md "32.7. Canceling Queries in Progress") | [Up](libpq.md "Chapter 32. libpq — C Library")|  [Next](libpq-notify.md "32.9. Asynchronous Notification")  
---|---|---  
32.7. Canceling Queries in Progress | [Home](index.md "PostgreSQL 17.5 Documentation")|  32.9. Asynchronous Notification
