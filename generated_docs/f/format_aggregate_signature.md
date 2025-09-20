# format_aggregate_signature

## Location
[src/bin/pg_dump/pg_dump.c:14195-14226](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L14195-L14226)

## Overview
Generates aggregate function name and argument list in a standardized signature format for use in SQL dump statements.

## Definition

```c
static char *
format_aggregate_signature(const AggInfo *agginfo, Archive *fout, bool honor_quotes)
```
## Detailed Description
The  function creates a formatted string representation of an aggregate function signature, including the function name and its argument types. This is used primarily for generating proper DROP and CREATE statements for aggregate functions during database dumps.

The function formats the signature as  or  for aggregates that accept any argument type. Argument type names are qualified if needed to avoid ambiguity. The aggregate name itself is never schema-qualified in the output.

## Parameters / Member Variables
- : AggInfo structure containing aggregate function metadata including name and argument types
- : Archive structure used for type name formatting context
- : Boolean flag controlling whether to apply identifier quoting to the aggregate name

## Dependencies
- Functions called/Symbols referenced:
  - initPQExpBuffer
  - [fmtId](fmtId.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - appendPQExpBufferChar
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [getFormattedTypeName](../g/getFormattedTypeName.md)
- Called from (representative examples):
  - [dumpAgg](../d/dumpAgg.md)

## Notes and Other Information
- Returns dynamically allocated string that caller must free
- Handles zero-argument aggregates with special  syntax
- Uses  to ensure proper type qualification
- The  parameter allows control over identifier quoting in different contexts
- Essential for generating syntactically correct aggregate function references in dump output