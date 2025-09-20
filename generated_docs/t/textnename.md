# textnename

## Location
[src/backend/utils/adt/varlena.c:2675-2699](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L2675-L2699)

## Overview
The  function implements the not-equal comparison operator between a text type and a name type in PostgreSQL.

## Definition

```c
Datum
textnename(PG_FUNCTION_ARGS)
```
## Detailed Description
This function compares a text value with a name (fixed-length string) and returns true if they are not equal. It mirrors the functionality of  but with reversed argument order. The function handles both C collation (simple byte comparison) and locale-aware collation using PostgreSQL's collation system. It extracts the lengths of both arguments and performs either a direct memory comparison for C collation or uses the  function for locale-aware comparison.

## Parameters / Member Variables
- : Text type argument (extracted using )
- : Name type argument (extracted using )

## Dependencies
- Functions called/Symbols referenced:
  - : Extract text argument with possible detoasting
  - : Extract name argument
  - : Get collation for comparison
  - : Validate collation is set
  - : Perform locale-aware string comparison
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/varlena.c:2675-2699
- Counterpart to  with reversed argument order
- Uses efficient memory comparison for C collation
- Properly handles variable-length text data with detoasting
- Returns the negation of equality comparison (not-equal operation)
- Frees copied text argument to prevent memory leaks