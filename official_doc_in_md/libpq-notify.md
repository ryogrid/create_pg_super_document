32.9. Asynchronous Notification  
---  
[Prev](libpq-fastpath.md "32.8. The Fast-Path Interface") | [Up](libpq.md "Chapter 32. libpq — C Library")| Chapter 32. libpq — C Library| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](libpq-copy.md "32.10. Functions Associated with the COPY Command")  
  
* * *

## 32.9. Asynchronous Notification #

PostgreSQL offers asynchronous notification via the `LISTEN` and `NOTIFY` commands. A client session registers its interest in a particular notification channel with the `LISTEN` command (and can stop listening with the `UNLISTEN` command). All sessions listening on a particular channel will be notified asynchronously when a `NOTIFY` command with that channel name is executed by any session. A “payload” string can be passed to communicate additional data to the listeners. 

libpq applications submit `LISTEN`, `UNLISTEN`, and `NOTIFY` commands as ordinary SQL commands. The arrival of `NOTIFY` messages can subsequently be detected by calling `PQnotifies`.

The function `PQnotifies` returns the next notification from a list of unhandled notification messages received from the server. It returns a null pointer if there are no pending notifications. Once a notification is returned from `PQnotifies`, it is considered handled and will be removed from the list of notifications. 
    
    
    PGnotify *PQnotifies(PGconn *conn);
    
    typedef struct pgNotify
    {
        char *relname;              /* notification channel name */
        int  be_pid;                /* process ID of notifying server process */
        char *extra;                /* notification payload string */
    } PGnotify;
    

After processing a `PGnotify` object returned by `PQnotifies`, be sure to free it with [`PQfreemem`](libpq-misc.md#LIBPQ-PQFREEMEM). It is sufficient to free the `PGnotify` pointer; the `relname` and `extra` fields do not represent separate allocations. (The names of these fields are historical; in particular, channel names need not have anything to do with relation names.) 

[Example 32.2](libpq-example.md#LIBPQ-EXAMPLE-2 "Example 32.2. libpq Example Program 2") gives a sample program that illustrates the use of asynchronous notification. 

`PQnotifies` does not actually read data from the server; it just returns messages previously absorbed by another libpq function. In ancient releases of libpq, the only way to ensure timely receipt of `NOTIFY` messages was to constantly submit commands, even empty ones, and then check `PQnotifies` after each [`PQexec`](libpq-exec.md#LIBPQ-PQEXEC). While this still works, it is deprecated as a waste of processing power. 

A better way to check for `NOTIFY` messages when you have no useful commands to execute is to call [`PQconsumeInput` ](libpq-async.md#LIBPQ-PQCONSUMEINPUT), then check `PQnotifies`. You can use `select()` to wait for data to arrive from the server, thereby using no CPU power unless there is something to do. (See [`PQsocket`](libpq-status.md#LIBPQ-PQSOCKET) to obtain the file descriptor number to use with `select()`.) Note that this will work OK whether you submit commands with [`PQsendQuery`](libpq-async.md#LIBPQ-PQSENDQUERY)/[`PQgetResult`](libpq-async.md#LIBPQ-PQGETRESULT) or simply use [`PQexec`](libpq-exec.md#LIBPQ-PQEXEC). You should, however, remember to check `PQnotifies` after each [`PQgetResult`](libpq-async.md#LIBPQ-PQGETRESULT) or [`PQexec`](libpq-exec.md#LIBPQ-PQEXEC), to see if any notifications came in during the processing of the command. 

* * *

[Prev](libpq-fastpath.md "32.8. The Fast-Path Interface") | [Up](libpq.md "Chapter 32. libpq — C Library")|  [Next](libpq-copy.md "32.10. Functions Associated with the COPY Command")  
---|---|---  
32.8. The Fast-Path Interface | [Home](index.md "PostgreSQL 17.5 Documentation")|  32.10. Functions Associated with the `COPY` Command
