# defGetCopyHeaderChoice

## Location
[src/backend/commands/copy.c:329-392](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copy.c#L329-L392)

## Overview
defGetCopyHeaderChoice extracts and validates a CopyHeaderChoice value from a DefElem parameter, supporting boolean values plus the special "match" option for COPY FROM operations.

## Definition

```c
static CopyHeaderChoice
defGetCopyHeaderChoice(DefElem *def, bool is_from)
```
## Detailed Description
This function parses and validates the HEADER option value for COPY statements. It extends standard boolean parsing to also accept the special "match" value, which is only valid for COPY FROM operations. The function handles various input formats including integers (0/1), boolean strings ("true"/"false", "on"/"off"), and the special "match" keyword. When no parameter value is provided, it defaults to COPY_HEADER_TRUE. The "match" option allows COPY FROM to automatically detect whether the input data has a header line by matching column names.

## Parameters / Member Variables
- : DefElem structure containing the parameter definition and value from the COPY statement's option list
- : Boolean flag indicating if this is a COPY FROM operation (true) or COPY TO operation (false)

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag
  - intVal
  - [defGetString](defGetString.md)
  - [pg_strcasecmp](../p/pg_strcasecmp.md)
  - ereport
  - COPY_HEADER_TRUE/FALSE/MATCH constants
- Called from (representative examples):
  - [ProcessCopyOptions](../P/ProcessCopyOptions.md)

## Notes and Other Information
- The "match" option is restricted to COPY FROM operations only and will generate an error if used with COPY TO
- Accepts the same string values as the grammar's opt_boolean_or_string production
- Provides comprehensive error reporting for invalid parameter values
- Returns COPY_HEADER_FALSE as a fallback to keep the compiler quiet, though this should never be reached due to error reporting