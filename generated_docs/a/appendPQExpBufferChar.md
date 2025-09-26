# appendPQExpBufferChar

## Location
[src/interfaces/libpq/pqexpbuffer.c:378-396](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/pqexpbuffer.c#L378-L396)

## Overview
Appends a single character to a PQExpBuffer string with optimized performance compared to the general appendPQExpBuffer function.

## Definition
```c
void appendPQExpBufferChar(PQExpBuffer str, char ch)
```

## Detailed Description
This function provides an optimized way to append a single character to a PQExpBuffer. It is specifically designed to be much faster than using appendPQExpBuffer(str, "%c", ch) for single character operations. The function first ensures the buffer has sufficient space by calling enlargePQExpBuffer, then directly adds the character to the buffer and updates the length and null terminator.

The function handles buffer expansion automatically and maintains proper null termination of the string. If buffer enlargement fails, the function returns early without modifying the buffer.

## Parameters / Member Variables
- `str`: The PQExpBuffer to append the character to
- `ch`: The character to append to the buffer

## Dependencies
- Functions called/Symbols referenced:
  - enlargePQExpBuffer
- Called from (representative examples):
  - replace_guc_value (src/bin/initdb/initdb.c)
  - indent_lines (src/bin/pg_amcheck/pg_amcheck.c)
  - appendConnStrItem (src/bin/pg_basebackup/pg_createsubscriber.c)
  - quoteAclUserName (src/bin/pg_dump/dumputils.c)
  - read_quoted_string (src/bin/pg_dump/filter.c)
  - fmtIdEnc (src/fe_utils/string_utils.c)
  - appendStringLiteralConn (src/fe_utils/string_utils.c)
  - build_client_first_message (src/interfaces/libpq/fe-auth-scram.c)

## Notes and Other Information
- This function is a performance optimization for single character appends
- The function automatically handles buffer growth when needed
- Maintains proper null termination of the buffer contents
- Widely used throughout PostgreSQL frontend utilities for string building operations
- Located in src/interfaces/libpq/pqexpbuffer.c:378-396