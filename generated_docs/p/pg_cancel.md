# pg_cancel

## Location
[src/interfaces/libpq/fe-cancel.c:40-64](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-cancel.c#L40-L64)

## Overview
The pg_cancel structure stores all data necessary to send a cancel request to a PostgreSQL backend process, including network addressing information and TCP connection parameters.

## Definition

```c
struct pg_cancel
{
	SockAddr	raddr;			/* Remote address */
	int			be_pid;			/* PID of to-be-canceled backend */
	int			be_key;			/* cancel key of to-be-canceled backend */
	int			pgtcp_user_timeout; /* tcp user timeout */
	int			keepalives;		/* use TCP keepalives? */
	int			keepalives_idle;	/* time between TCP keepalives */
	int			keepalives_interval;	/* time between TCP keepalive
										 * retransmits */
	int			keepalives_count;	/* maximum number of TCP keepalive
									 * retransmits */
};
```
## Detailed Description
The pg_cancel structure is the backing struct for the opaque PGcancel type exposed through libpq-fe.h. It encapsulates all the information required to establish a connection to a PostgreSQL backend and send a query cancellation request. The structure contains the essential identification information (backend PID and cancel key) needed for the cancellation protocol, along with comprehensive TCP connection parameters that control timeout behavior and keepalive settings. This allows cancellation requests to be sent reliably even under various network conditions.

## Parameters / Member Variables
- `raddr`: Socket address structure containing the remote server's network address information
- `be_pid`: Process ID of the backend process that should be canceled
- `be_key`: Secret cancellation key associated with the backend process for security
- `pgtcp_user_timeout`: TCP user timeout value controlling overall connection timeout behavior
- `keepalives`: Boolean flag indicating whether TCP keepalive probes should be used
- `keepalives_idle`: Time interval (in seconds) between TCP keepalive probes when the connection is idle
- `keepalives_interval`: Time interval (in seconds) between TCP keepalive retransmissions
- `keepalives_count`: Maximum number of TCP keepalive retransmissions before considering the connection dead

## Dependencies
- Functions called/Symbols referenced:
  - [SockAddr](../S/SockAddr.md) (network address structure)
  - [PGcancelConn](../P/PGcancelConn.md) (related cancellation connection structure)
- Called from (representative examples):
  - [PGcancel](../P/PGcancel.md) (typedef in libpq-fe.h)
  - [Query](../Q/Query.md) cancellation functions in libpq

## Notes and Other Information
- This structure is internal to libpq and not directly accessible to client applications
- The contents are manipulated through the opaque PGcancel typedef and related API functions
- The backend PID and cancel key must match the target backend for successful cancellation
- TCP keepalive parameters provide robustness for cancellation requests over unreliable networks
- Part of PostgreSQL's secure query cancellation mechanism that prevents unauthorized query termination
- The structure design ensures cancellation requests can be sent independently of the main database connection