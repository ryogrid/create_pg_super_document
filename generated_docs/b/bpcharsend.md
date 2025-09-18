# bpcharsend

## Location
[src/backend/utils/adt/varchar.c:251-270](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varchar.c#L251-L270)

## Overview
Converts the PostgreSQL bpchar (blank-padded character) data type to external binary format for data transmission.

## Definition
```c
Datum bpcharsend(PG_FUNCTION_ARGS)
```

## Detailed Description
The `bpcharsend` function is a PostgreSQL I/O function that handles the conversion of bpchar values to binary format for external transmission. This function is part of PostgreSQL's binary protocol support, enabling efficient transmission of character data between server and clients.

Notably, this function simply delegates to `textsend` since the binary representation of bpchar and text types are identical. This design choice reflects the fact that both types store variable-length character data in the same underlying format, differing only in their semantic handling (bpchar includes blank-padding semantics).

## Parameters / Member Variables
- `fcinfo` (FunctionCallInfo): Function call information structure containing the bpchar value to be converted

## Dependencies
- Functions called/Symbols referenced:
  - [textsend](../t/textsend.md)
- Called from (representative examples):
  - None found in current analysis

## Notes and Other Information
- This function follows PostgreSQL's standard function calling convention using PG_FUNCTION_ARGS
- The implementation reuses textsend code, demonstrating PostgreSQL's efficient code sharing between related data types
- Part of the binary I/O protocol infrastructure for the bpchar data type
- The binary format for bpchar is identical to text, reflecting their similar underlying storage