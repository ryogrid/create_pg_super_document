# pg_conn_host

## Location
src/interfaces/libpq/libpq-int.h: 349 - 359

## Overview
A structure that stores information about each host mentioned in PostgreSQL connection strings, supporting multiple hosts for connection failover and load balancing scenarios.

## Definition

```c
typedef struct pg_conn_host
{
	pg_conn_host_type type;		/* type of host address */
	char	   *host;			/* host name or socket path */
	char	   *hostaddr;		/* host numeric IP address */
	char	   *port;			/* port number (if NULL or empty, use
								 * DEF_PGPORT[_STR]) */
	char	   *password;		/* password for this host, read from the
								 * password file; NULL if not sought or not
								 * found in password file. */
} pg_conn_host;
```
## Detailed Description
The  structure is designed to handle PostgreSQL's multi-host connection capability. When a connection string contains multiple hosts separated by commas, each host's information is parsed and stored in a separate  structure. This enables connection failover where the client can attempt to connect to alternative hosts if the primary host is unavailable.

The structure supports three types of host addressing: host names, IP addresses, and Unix domain sockets. Each host can have its own port number and password, allowing for flexible connection configurations across different database servers.

## Parameters / Member Variables
- : Enum value indicating the type of host address (CHT_HOST_NAME, CHT_HOST_ADDRESS, or CHT_UNIX_SOCKET)
- : String containing either a hostname, IP address, or Unix socket path depending on the connection type
- : String containing the numeric IP address representation of the host
- : String containing the port number; if NULL or empty, defaults to DEF_PGPORT or DEF_PGPORT_STR
- : String containing the password for this specific host, typically read from a password file; NULL if not available

## Dependencies
- Functions called/Symbols referenced:
  - pg_conn_host_type (enum for host type classification)
- Called from (representative examples):
  - pqConnectOptions2 (in fe-connect.c for parsing connection options)
  - PQconnectPoll (in fe-connect.c for connection polling)
  - PQcancelCreate (in fe-cancel.c for cancellation requests)
  - pg_conn (main connection structure)

## Notes and Other Information
- Part of the libpq internal interface, defined in libpq-int.h:349-359
- Supports PostgreSQL's multi-host connection strings for high availability
- Password field is populated from password files when available, enhancing security by not requiring passwords in connection strings
- The structure is primarily used during connection establishment and is referenced by the main pg_conn structure
- Host type classification enables proper handling of different addressing schemes (DNS names, IP addresses, Unix sockets)