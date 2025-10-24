# PQisthreadsafe

## Location
[src/interfaces/libpq/fe-exec.c:3992-3999](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L3992-L3999)

## Overview
Returns whether the libpq library was compiled with thread safety support, indicating if it's safe to use libpq functions from multiple threads simultaneously.

## Definition
```c
int PQisthreadsafe(void)
```

## Detailed Description
PQisthreadsafe is a simple query function that indicates whether the current build of libpq supports thread-safe operations. In modern PostgreSQL builds, this function always returns true (non-zero), indicating that libpq is compiled with thread safety enabled.

Thread safety in libpq means that:
- Multiple threads can safely use different PGconn connection objects simultaneously
- The library's internal data structures are protected against concurrent access
- Global libpq state is properly synchronized across threads

However, it's important to note that individual PGconn objects are not inherently thread-safe - each connection should still be used by only one thread at a time, or access must be synchronized by the application.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - None (returns constant value)
- Called from (representative examples):
  - Referenced in libpq-fe.h header declarations

## Notes and Other Information
- Always returns true (non-zero) in current PostgreSQL versions
- This function exists primarily for backward compatibility and feature detection
- Thread safety applies to the library level, not individual connection objects
- Applications should still synchronize access to individual PGconn objects across threads
- The function takes no parameters and has no side effects
- Useful for applications that need to verify thread safety capability at runtime

## Simplified Source

```c
int PQisthreadsafe(void) {
    // libpq is always thread-safe in modern PostgreSQL
    return true;
}
```