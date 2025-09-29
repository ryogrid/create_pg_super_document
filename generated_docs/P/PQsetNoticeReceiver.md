# PQsetNoticeReceiver

## Location
[src/interfaces/libpq/fe-connect.c:7321-7337](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L7321-L7337)

## Overview
Sets a callback function to receive notice messages from the PostgreSQL server, allowing applications to handle server notices and warnings in a custom manner.

## Definition


Where PQnoticeReceiver is defined as:


## Detailed Description
PQsetNoticeReceiver allows applications to install a custom callback function that will be called whenever the PostgreSQL server sends notice messages (such as warnings, informational messages, or debug output). The function replaces any previously installed notice receiver with the new callback function and associated argument. Notice messages are distinct from error messages and are typically used for non-fatal warnings or informational output from the server.

## Parameters / Member Variables
- : The database connection handle (if NULL, returns NULL)
- : Pointer to the callback function that will receive notice messages (or NULL to disable notice handling)
- : User-defined argument that will be passed to the callback function

## Dependencies
- Functions called/Symbols referenced:
  - PQnoticeReceiver (function pointer type)
- Called from (representative examples):
  - [ECPGconnect](../E/ECPGconnect.md) (in ECPG connect.c)

## Notes and Other Information
- Returns the previous notice receiver function pointer, allowing for restoration
- If conn is NULL, the function returns NULL safely
- The callback function receives two parameters: the user-defined arg and a PGresult containing the notice
- Setting proc to NULL disables notice processing for the connection
- Notice messages are typically non-fatal warnings, debug information, or server status messages
- The notice receiver is called synchronously when notices are received from the server
- Applications should avoid performing lengthy operations in the notice receiver callback
- The PGresult passed to the callback should not be freed by the application

## Simplified Source

```c
PQnoticeReceiver PQsetNoticeReceiver(PGconn *conn, PQnoticeReceiver proc, void *arg) {
    PQnoticeReceiver old;

    // Handle null connection safely
    if (conn == NULL)
        return NULL;

    // Store previous notice receiver for return
    old = conn->noticeHooks.noticeRec;

    // Install new notice receiver if provided
    if (proc) {
        conn->noticeHooks.noticeRec = proc;
        conn->noticeHooks.noticeRecArg = arg;
    }

    return old;
}
```