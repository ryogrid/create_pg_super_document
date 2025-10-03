# store_conn_addrinfo

## Location
[src/interfaces/libpq/fe-connect.c:4745-4783](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L4745-L4783)

## Overview
Copies network address information from a system addrinfo linked list into a PGconn object's internal address array for libpq manipulation.

## Definition

```c
static int
store_conn_addrinfo(PGconn *conn, struct addrinfo *addrlist)
```
## Detailed Description
The  function is responsible for converting and storing network address information from the system's  linked list format into libpq's internal  array format within a PGconn structure. This conversion is necessary to allow libpq to manage and manipulate connection address data independently of the system's address resolution results.

The function operates in two passes:
1. **Counting pass**: Traverses the  linked list to determine the total number of addresses
2. **Copying pass**: Allocates an array of  structures and copies address data from each  entry

Each address entry includes the address family (IPv4/IPv6) and the actual socket address data with its length. The function initializes the  field to 0, which is used by libpq to track which address in the array is currently being used for connection attempts.

## Parameters / Member Variables
- `*conn`: Pointer to the PGconn structure where address information will be stored
- `*addrlist`: Pointer to the head of a linked list of  structures containing resolved addresses
## Dependencies
- Functions called/Symbols referenced:
  -  (memory allocation)
  -  (memory copying)
  -  (error reporting)
  -  (libpq's internal address structure type)

- Called from (representative examples):
  - 
  - 

## Notes and Other Information
- Returns 0 on success, 1 on failure (memory allocation error)
- Initializes  to 0 to start with the first address in the array
- Uses  to ensure the address array is zero-initialized
- Copies both the address family and the complete socket address structure
- The  field stores the length of the socket address for proper handling
- Memory allocation failure results in an "out of memory" error message being appended to the connection
- The function assumes the input  is a valid linked list (handles NULL termination correctly)
- The resulting  array allows libpq to iterate through multiple addresses during connection attempts
- This function is part of the connection establishment process where DNS resolution results are stored for later use
- The copied address information persists in the PGconn until the connection is cleaned up

## Simplified Source
```c
static int store_conn_addrinfo(PGconn *conn, struct addrinfo *addrlist) {
    struct addrinfo *ai = addrlist;

    conn->whichaddr = 0;

    // Count addresses in the linked list
    conn->naddr = 0;
    while (ai) {
        ai = ai->ai_next;
        conn->naddr++;
    }

    // Allocate array for addresses
    conn->addr = calloc(conn->naddr, sizeof(AddrInfo));
    if (conn->addr == NULL) {
        libpq_append_conn_error(conn, "out of memory");
        return 1;
    }

    // Copy address data to internal array
    ai = addrlist;
    for (int i = 0; i < conn->naddr; i++) {
        conn->addr[i].family = ai->ai_family;
        memcpy(&conn->addr[i].addr.addr, ai->ai_addr, ai->ai_addrlen);
        conn->addr[i].addr.salen = ai->ai_addrlen;
        ai = ai->ai_next;
    }

    return 0;
}
```