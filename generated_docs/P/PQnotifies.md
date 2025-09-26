# PQnotifies

## Location
[src/interfaces/libpq/fe-exec.c:2667-2694](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L2667-L2694)

## Overview
PQnotifies retrieves and returns the next unhandled asynchronous notification from the PostgreSQL server, removing it from the connection's internal notification queue.

## Definition
```c
PGnotify *PQnotifies(PGconn *conn)
```

## Detailed Description
PQnotifies provides access to asynchronous NOTIFY messages sent by the PostgreSQL server. The function retrieves the oldest unhandled notification from the connection's internal notification queue and returns it to the application. The notification is removed from the queue and ownership is transferred to the caller, who becomes responsible for freeing the returned structure.

The function first attempts to parse any available input data to extract NOTIFY messages that may have arrived from the server. It then removes and returns the head of the notification queue. If no notifications are pending, the function returns NULL.

Important: This function does not read new data from the socket - applications should typically call PQconsumeInput() first to ensure all available data has been processed.

## Parameters / Member Variables
- `conn`: Connection handle to the PostgreSQL database server

## Dependencies
- Functions called/Symbols referenced:
  - [parseInput](../p/parseInput.md)
  - PGnotify
- Called from (representative examples):
  - [PrintNotifications](PrintNotifications.md) (in psql)
  - [ecpg_process_output](../e/ecpg_process_output.md) (in ECPG)
  - [main](../m/main.md) (in testlibpq2 example)
  - [try_complete_step](../t/try_complete_step.md) (in isolation tester)

## Notes and Other Information
- Returns a PGnotify pointer to the notification structure, or NULL if no notifications are pending
- The caller is responsible for freeing the returned PGnotify structure
- Does not read new data from the socket - call PQconsumeInput() first to ensure all data is processed  
- Removes the notification from the internal queue, transferring ownership to the caller
- Part of PostgreSQL's asynchronous notification system (LISTEN/NOTIFY)
- The returned structure's 'next' pointer is set to NULL to hide internal queue state from applications
- Used primarily for handling PostgreSQL NOTIFY messages sent via the NOTIFY SQL command