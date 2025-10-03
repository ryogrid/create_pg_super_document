# fmtCopyColumnList

## Location
[src/bin/pg_dump/pg_dump.c:18993-19026](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L18993-L19026)

## Overview
Generates a parenthesized column list clause for COPY commands, excluding dropped and generated columns from the specified table.

## Definition

```c
static const char *
fmtCopyColumnList(const TableInfo *ti, PQExpBuffer buffer)
```
## Detailed Description
This function constructs a properly formatted column list for use in COPY statements by iterating through a table's attributes and including only regular (non-dropped, non-generated) columns. The function formats the output as a parenthesized, comma-separated list of properly quoted column names. It includes special handling for the edge case where no valid columns exist, returning an empty string instead of invalid empty parentheses.

## Parameters / Member Variables
- `*ti`: TableInfo structure containing table metadata including column information
- `buffer`: PQExpBuffer used to construct the formatted column list
## Dependencies
- Functions called/Symbols referenced:
  - [appendPQExpBufferChar](../a/appendPQExpBufferChar.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [fmtId](fmtId.md)
- Types used:
  - [TableInfo](../T/TableInfo.md)
- Called from (representative examples):
  - [dumpTableData_copy](../d/dumpTableData_copy.md)
  - [dumpTableData](../d/dumpTableData.md)

## Notes and Other Information
- Excludes dropped columns (attisdropped[i] == true)
- Excludes generated columns (attgenerated[i] == true)
- Uses fmtId() to properly quote column names that need escaping
- Returns empty string "" if no valid columns exist (avoids invalid "()" syntax)
- [Result](../R/Result.md) is stored in the provided buffer and returned as buffer->data
- Essential for generating correct COPY statements during pg_dump operations