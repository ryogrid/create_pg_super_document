# conninfo_init

## Location
[src/interfaces/libpq/fe-connect.c:5760-5798](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L5760-L5798)

## Overview
Builds a working copy of the constant PQconninfoOptions array for dynamic manipulation during connection parameter parsing.

## Definition
```c
static PQconninfoOption *conninfo_init(PQExpBuffer errorMessage)
```

## Detailed Description
This function creates a dynamically allocated copy of the global PQconninfoOptions array, which contains the template definitions for all supported PostgreSQL connection parameters. The function serves as an initialization step for connection parameter parsing, providing a mutable array that can be populated with actual values during the parsing process.

The function carefully copies only the public portion of each option structure (PQconninfoOption) from the internal template array (internalPQconninfoOption), filtering out internal-only fields. This ensures that the returned array contains only the information that should be exposed to client applications.

## Parameters / Member Variables
- `errorMessage`: Buffer for storing error messages if memory allocation fails

## Dependencies
- Functions called/Symbols referenced:
  - malloc
  - [libpq_append_error](../l/libpq_append_error.md)
  - memcpy
  - MemSet
  - [PQconninfoOption](../P/PQconninfoOption.md) (data structure)
  - internalPQconninfoOption (data structure)
  - PQconninfoOptions (global array)
- Called from (representative examples):
  - internalPQconninfoOption
  - PQconndefaults
  - [conninfo_parse](conninfo_parse.md)
  - [conninfo_array_parse](conninfo_array_parse.md)
  - [conninfo_uri_parse](conninfo_uri_parse.md)
  - [PQconninfo](../P/PQconninfo.md)

## Notes and Other Information
- Returns NULL on memory allocation failure
- The returned array is null-terminated with a zeroed PQconninfoOption structure
- Memory allocation size is calculated based on the size of the PQconninfoOptions array
- Only copies the public fields of connection options, hiding internal implementation details
- The caller is responsible for freeing the returned array when no longer needed
- Essential building block for all connection parameter parsing functions in libpq