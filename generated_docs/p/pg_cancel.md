# pg_cancel

## Location
[src/interfaces/libpq/fe-cancel.c:40-64](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-cancel.c#L40-L64)

## Overview
The pg_cancel structure stores all data necessary to send a cancel request to a PostgreSQL backend process, including network addressing information and TCP connection parameters.

## Definition


## Detailed Description
The pg_cancel structure is the backing struct for the opaque PGcancel type exposed through libpq-fe.h. It encapsulates all the information required to establish a connection to a PostgreSQL backend and send a query cancellation request. The structure contains the essential identification information (backend PID and cancel key) needed for the cancellation protocol, along with comprehensive TCP connection parameters that control timeout behavior and keepalive settings. This allows cancellation requests to be sent reliably even under various network conditions.

## Parameters / Member Variables
- : Socket address structure containing the remote server's network address information
- : Process ID of the backend process that should be canceled
- : Secret cancellation key associated with the backend process for security
- : TCP user timeout value controlling overall connection timeout behavior
- : Boolean flag indicating whether TCP keepalive probes should be used
- : Time interval (in seconds) between TCP keepalive probes when the connection is idle
- : Time interval (in seconds) between TCP keepalive retransmissions
- : Maximum number of TCP keepalive retransmissions before considering the connection dead

## Dependencies
- Functions called/Symbols referenced:
  - [SockAddr](../S/SockAddr.md) (network address structure)
  - PGcancelConn (related cancellation connection structure)
- Called from (representative examples):
  - PGcancel (typedef in libpq-fe.h)
  - [Query](../Q/Query.md) cancellation functions in libpq

## Notes and Other Information
- This structure is internal to libpq and not directly accessible to client applications
- The contents are manipulated through the opaque PGcancel typedef and related API functions
- The backend PID and cancel key must match the target backend for successful cancellation
- TCP keepalive parameters provide robustness for cancellation requests over unreliable networks
- Part of PostgreSQL's secure query cancellation mechanism that prevents unauthorized query termination
- The structure design ensures cancellation requests can be sent independently of the main database connection