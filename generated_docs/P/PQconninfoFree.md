# PQconninfoFree

## Location
[src/interfaces/libpq/fe-connect.c:6990-7002](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L6990-L7002)

## Overview
A public libpq API function that properly deallocates memory for a PQconninfoOption array and all its associated string values.

## Definition

```c
void
PQconninfoFree(PQconninfoOption *connOptions)
```
## Detailed Description
This function provides proper memory management for PQconninfoOption arrays returned by functions like PQconninfo(), PQconndefaults(), and PQconninfoParse(). It performs a complete cleanup by:

1. **Null Check**: Safely handles NULL input by returning immediately
2. **Value Cleanup**: Iterates through all options in the array and frees each option's value string
3. **Array Cleanup**: Frees the array structure itself

The function ensures that all dynamically allocated memory associated with connection options is properly released, preventing memory leaks in applications that use libpq connection introspection features.

## Parameters / Member Variables
- `connOptions`: Array of PQconninfoOption structures to be freed (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - free
- Called from (representative examples):
  - [libpqrcv_check_conninfo](../l/libpqrcv_check_conninfo.md)
  - [GetConnection](../G/GetConnection.md) (pg_basebackup tools)
  - [connectDatabase](../c/connectDatabase.md) (pg_dumpall)
  - [do_connect](../d/do_connect.md) (psql)
  - [PQconnectStartParams](PQconnectStartParams.md)
  - [conninfo_parse](../c/conninfo_parse.md)
  - [conninfo_array_parse](../c/conninfo_array_parse.md)

## Notes and Other Information
- This is a public libpq API function exposed to client applications
- Safe to call with NULL pointer - function will return immediately without error
- Must be called for every PQconninfoOption array obtained from libpq functions to prevent memory leaks
- Frees both the individual value strings and the array structure itself
- Essential counterpart to functions like PQconninfo() and PQconndefaults()
- Widely used throughout PostgreSQL tools and applications for proper memory management
- Simple but critical function for maintaining memory hygiene in libpq applications