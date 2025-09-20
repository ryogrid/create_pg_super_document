# textregexreplace_extended_no_flags

## Location
[src/backend/utils/adt/regexp.c:751-766](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L751-L766)

## Overview
A wrapper function that delegates to textregexreplace_extended, created specifically to satisfy PostgreSQL's operator sanity regression tests for function signatures without flags parameter.

## Definition

```c
Datum
textregexreplace_extended_no_flags(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a simple wrapper that directly calls  with the same function call information (). Like its companion , this function exists purely to satisfy PostgreSQL's opr_sanity regression test requirements.

The function provides the same functionality as  but represents a different function signature in PostgreSQL's catalog system - specifically for cases where the flags parameter is not provided. This allows PostgreSQL's function resolution system to distinguish between different overloads while maintaining a unified implementation.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  -  (performs the actual implementation)
- Called from (representative examples):
  - No direct callers found (likely called through PostgreSQL's function dispatcher)

## Notes and Other Information
- Created specifically to address opr_sanity regression test requirements
- Acts as a pass-through wrapper with no additional logic
- Maintains identical behavior to textregexreplace_extended
- Part of PostgreSQL's function overloading mechanism for regexp_replace variants
- The comment indicates this separation is purely for testing compliance
- Represents a function signature variant without explicit flags parameter
- Uses PostgreSQL's standard function call information structure (fcinfo) for argument passing