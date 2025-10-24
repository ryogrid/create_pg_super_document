# pqBuildStartupPacket3

## Location
[src/interfaces/libpq/fe-protocol3.c:2237-2259](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-protocol3.c#L2237-L2259)

## Overview
Constructs a PostgreSQL protocol 3 startup packet by allocating memory and delegating the actual packet construction to the build_startup_packet helper function.

## Definition

```c
char *
pqBuildStartupPacket3(PGconn *conn, int *packetlen,
					  const PQEnvironmentOption *options)
```
## Detailed Description
pqBuildStartupPacket3 serves as a memory-allocating wrapper around the build_startup_packet function. It first calls build_startup_packet with a NULL buffer to determine the required packet length, then allocates the necessary memory, and finally calls build_startup_packet again with the allocated buffer to construct the actual startup packet. This two-phase approach ensures efficient memory allocation by determining the exact size needed before allocation.

## Parameters / Member Variables
- `*conn`: PostgreSQL connection handle containing connection parameters
- `*packetlen`: Pointer to store the length of the constructed packet
- `*options`: Array of PQEnvironmentOption structures containing environment-specific connection options
## Dependencies
- Functions called/Symbols referenced:
  - [build_startup_packet](../b/build_startup_packet.md) (core packet construction function)
  - malloc (memory allocation)
  - [PQEnvironmentOption](../P/PQEnvironmentOption.md) (structure type for environment options)
- Called from (representative examples):
  - CONNECTION_FAILED state handling (in src/interfaces/libpq/fe-connect.c)

## Notes and Other Information
- Returns a malloc'd packet buffer that must be freed by the caller, or NULL if memory allocation fails
- Uses a two-pass approach: first call determines packet size, second call fills the allocated buffer
- Part of the libpq protocol 3 implementation for PostgreSQL client-server communication
- The actual packet construction logic is delegated to the build_startup_packet helper function
- Memory allocation failure is handled by returning NULL, allowing callers to detect and handle out-of-memory conditions

## Simplified Source

```c
char *pqBuildStartupPacket3(PGconn *conn, int *packetlen,
                           const PQEnvironmentOption *options) {
    // First pass: determine required packet size
    *packetlen = build_startup_packet(conn, NULL, options);

    // Allocate memory for the packet
    char *startpacket = (char *) malloc(*packetlen);
    if (!startpacket)
        return NULL;

    // Second pass: fill the allocated buffer with packet data
    *packetlen = build_startup_packet(conn, startpacket, options);
    return startpacket;
}
```