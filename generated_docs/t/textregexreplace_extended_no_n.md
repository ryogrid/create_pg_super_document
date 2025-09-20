# textregexreplace_extended_no_n

## Location
[src/backend/utils/adt/regexp.c:744-750](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L744-L750)

## Overview
A wrapper function that delegates to textregexreplace_extended, created specifically to satisfy PostgreSQL's operator sanity regression tests.

## Definition

```c
Datum
textregexreplace_extended_no_n(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a simple wrapper that directly calls  with the same function call information (). This function exists solely to satisfy PostgreSQL's opr_sanity regression test requirements, which expects distinct function implementations for different SQL function signatures.

Despite being a separate function, it provides identical functionality to  by passing through all arguments unchanged. This design pattern allows PostgreSQL's catalog system to distinguish between different function overloads while maintaining a single implementation.

## Parameters / Member Variables
- Supports the same parameter set as the extended version: source text, pattern, replacement, optional start position, and optional flags

## Dependencies
- Functions called/Symbols referenced:
  -  (performs the actual implementation)
- Called from (representative examples):
  - No direct callers found (likely called through PostgreSQL's function dispatcher)

## Notes and Other Information
- Created specifically to address opr_sanity regression test requirements
- Acts as a pass-through wrapper with no additional logic
- Maintains the same behavior and capabilities as textregexreplace_extended
- Part of PostgreSQL's function overloading mechanism for regexp_replace variants
- The comment indicates this separation is purely for testing compliance
- Uses PostgreSQL's standard function call information structure (fcinfo) for argument passing