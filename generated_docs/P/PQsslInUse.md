# PQsslInUse

## Location
[src/interfaces/libpq/fe-secure.c:103-114](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure.c#L103-L114)

## Overview
Determines whether an SSL connection is currently in use for a PostgreSQL connection.

## Definition

```c
int
PQsslInUse(PGconn *conn)
```
## Detailed Description
PQsslInUse is a simple utility function that checks if SSL encryption is active on a given PostgreSQL connection. It provides a way for client applications to verify that their connection is secured with SSL/TLS encryption. The function simply returns the value of the  flag from the connection structure.

## Parameters / Member Variables
- `*conn`: Pointer to the PGconn connection structure to check
## Dependencies
- Functions called/Symbols referenced: None
- Called from (representative examples):
  - [printSSLInfo](../p/printSSLInfo.md) (in src/bin/psql/command.c:3978)
  - Referenced in PQsetdb header (src/interfaces/libpq/libpq-fe.h:408)

## Notes and Other Information
- Returns 0 if the connection pointer is NULL or if SSL is not in use
- Returns non-zero (specifically the value of conn->ssl_in_use) if SSL is active
- This is a read-only query function that does not modify the connection state
- Commonly used by client applications to verify security status before transmitting sensitive data