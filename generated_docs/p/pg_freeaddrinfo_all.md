# pg_freeaddrinfo_all

## Location
[src/common/ip.c:82-113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/ip.c#L82-L113)

## Overview
Provides a unified interface for freeing addrinfo structures allocated by either the system's getaddrinfo() or PostgreSQL's custom getaddrinfo_unix() function.

## Definition
```c
void pg_freeaddrinfo_all(int hint_ai_family, struct addrinfo *ai)
```

## Detailed Description
This function serves as the cleanup counterpart to pg_getaddrinfo_all(), properly deallocating memory based on how the addrinfo structure was originally allocated. It uses the original hint's ai_family field to determine the allocation method, since some getaddrinfo() implementations might return AF_UNIX addresses, making it unsafe to rely on the ai_family field within the addrinfo structure itself.

For Unix domain sockets (AF_UNIX), it manually frees each node in the linked list, deallocating both the ai_addr and the addrinfo structure itself. For network sockets, it delegates to the system's freeaddrinfo() function.

## Parameters / Member Variables
- `hint_ai_family`: The ai_family value from the original hint structure passed to pg_getaddrinfo_all()
- `ai`: Pointer to the addrinfo structure(s) to be freed

## Dependencies
- Functions called/Symbols referenced:
  - free (standard C library function)
  - freeaddrinfo (standard system function)
- Called from (representative examples):
  - [ident_inet](../i/ident_inet.md) (authentication cleanup)
  - [PerformRadiusTransaction](../P/PerformRadiusTransaction.md) (RADIUS transaction cleanup)
  - [parse_hba_line](parse_hba_line.md) (HBA configuration parsing cleanup)
  - [ListenServerPort](../L/ListenServerPort.md) (server setup cleanup)
  - [PQconnectPoll](../P/PQconnectPoll.md) (client connection cleanup)

## Notes and Other Information
- Critical that the hint_ai_family parameter matches the original hint used with pg_getaddrinfo_all()
- Handles linked lists of addrinfo structures for Unix sockets by iterating through ai_next pointers
- Safe to call with NULL ai pointer (checked before calling system freeaddrinfo)
- Manual memory management for Unix sockets reflects custom allocation in getaddrinfo_unix()
- Located in src/common/ip.c:82-113, available to both frontend and backend code