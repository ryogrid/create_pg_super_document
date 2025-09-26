# fmtQualifiedIdEnc

## Location
[src/fe_utils/string_utils.c:263-295](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/string_utils.c#L263-L295)

## Overview
Constructs a schema-qualified identifier name with proper quoting based on encoding requirements, used in PostgreSQL frontend utilities for safe identifier formatting.

## Definition
```c
const char *fmtQualifiedIdEnc(const char *schema, const char *id, int encoding)
```

## Detailed Description
The `fmtQualifiedIdEnc` function creates a properly formatted schema-qualified identifier (e.g., "schema.table") with appropriate quoting applied based on the specified encoding. It handles cases where the schema may be NULL or empty, in which case only the identifier is returned. The function uses `fmtIdEnc` internally to ensure both the schema and identifier components are properly quoted according to SQL standards and encoding requirements.

The function uses a local PQExpBuffer for intermediate processing and returns the result via a global buffer obtained from `getLocalPQExpBuffer()`. This means the result should be used immediately or copied before making another call to this function or related formatting functions.

## Parameters / Member Variables
- `schema`: Schema name to prepend to the identifier (can be NULL or empty)
- `id`: The identifier name to be formatted (required)
- `encoding`: Character encoding specification for proper quoting decisions

## Dependencies
- Functions called/Symbols referenced:
  - [fmtIdEnc](fmtIdEnc.md) (called twice - once for schema, once for id)
  - `[createPQExpBuffer](../c/createPQExpBuffer.md)`
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - `getLocalPQExpBuffer`
  - `[destroyPQExpBuffer](../d/destroyPQExpBuffer.md)`
- Called from (representative examples):
  - [appendQualifiedRelation](../a/appendQualifiedRelation.md) (src/bin/scripts/common.c:115)
  - [get_parallel_object_list](../g/get_parallel_object_list.md) (src/bin/scripts/reindexdb.c:791, 805)
  - [vacuum_one_database](../v/vacuum_one_database.md) (src/bin/scripts/vacuumdb.c:788)
  - [fmtQualifiedId](fmtQualifiedId.md) (src/fe_utils/string_utils.c:298)

## Notes and Other Information
- The function gracefully handles NULL or empty schema names by omitting the schema prefix
- Uses temporary PQExpBuffer to avoid conflicts with the global buffer used by `fmtIdEnc`
- [Result](../R/Result.md) should be used immediately as it may be overwritten by subsequent calls
- Part of the frontend utilities string formatting infrastructure
- Encoding parameter allows for proper character handling in different database encodings