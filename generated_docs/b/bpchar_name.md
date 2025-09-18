# bpchar_name

## Location
[src/backend/utils/adt/varchar.c:371-406](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varchar.c#L371-L406)

## Overview
Converts a bpchar (blank-padded character) value to a NameData type, handling truncation and trailing space removal for PostgreSQL system identifiers.

## Definition
```c
Datum bpchar_name(PG_FUNCTION_ARGS)
```

## Detailed Description
The `bpchar_name` function converts a bpchar value to PostgreSQL's NameData type, which is used for system identifiers such as table names, column names, and other database object names. The function handles the necessary transformations including length truncation to fit within NAMEDATALEN limits and removal of trailing blanks that are semantically meaningless for identifiers.

The conversion process involves several steps:
1. Extract the character data from the bpchar input
2. Truncate to NAMEDATALEN-1 if the input exceeds the name length limit
3. Remove trailing spaces since names shouldn't have trailing blanks
4. Zero-pad the result to ensure proper null-termination and consistent memory layout

The function uses multi-byte character awareness when clipping oversize input to avoid splitting characters at byte boundaries.

## Parameters / Member Variables
- `s` (BpChar*): Input bpchar value to be converted to NameData
- `s_data` (char*): Pointer to the character data within the bpchar
- `result` (Name): Output NameData structure
- `len` (int): Working length variable for data processing

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BPCHAR_PP
  - VARSIZE_ANY_EXHDR
  - VARDATA_ANY
  - [pg_mbcliplen](../p/pg_mbcliplen.md)
  - [palloc0](../p/palloc0.md)
  - memcpy
  - NameStr
  - PG_RETURN_NAME
- Called from (representative examples):
  - None found in current analysis

## Notes and Other Information
- Essential for converting user-provided character data to PostgreSQL system identifier format
- Handles multi-byte character encodings properly to avoid character splitting during truncation
- Uses palloc0 to ensure the result is zero-padded, which is important for NameData consistency
- Trailing space removal reflects the semantic difference between padded character data and identifiers
- The NAMEDATALEN limit reflects PostgreSQL's internal constraints on identifier lengths