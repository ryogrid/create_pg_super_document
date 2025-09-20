# be_lo_get

## Location
[src/backend/libpq/be-fsstubs.c:792-805](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-fsstubs.c#L792-L805)

## Overview
A PostgreSQL backend function that reads and returns the entire content of a large object (LO) as bytea data.

## Definition

```c
Datum
be_lo_get(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL backend function that retrieves the complete content of a large object identified by its OID. It serves as a wrapper around the internal  function, requesting the entire object by specifying an offset of 0 and length of -1 (indicating all remaining data). The function is part of PostgreSQL's large object support system, which allows storing and manipulating binary data that exceeds the normal size limits of regular data types.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that contains:
  -  (Oid): The object identifier of the large object to read

## Dependencies
- Functions called/Symbols referenced:
  - : Internal function that performs the actual large object reading
  - : Macro to extract OID argument from function call
  - : Macro to return bytea result
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL function interface)

## Notes and Other Information
- The function reads the entire large object content in a single operation
- Uses offset 0 and length -1 to indicate reading from start to end
- Returns the data as a bytea type, suitable for binary content
- Part of PostgreSQL's large object filesystem stub interface
- Located in src/backend/libpq/be-fsstubs.c:792-805