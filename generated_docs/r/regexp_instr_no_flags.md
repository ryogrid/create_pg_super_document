# regexp_instr_no_flags

## Location
src/backend/utils/adt/regexp.c: 1266 - 1272

## Overview
A PostgreSQL wrapper function that provides the regexp_instr functionality without requiring explicit flags parameter to maintain compatibility with the opr_sanity regression test.

## Definition
```c
Datum regexp_instr_no_flags(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a thin wrapper around the main `regexp_instr` function. It was created specifically to keep the opr_sanity regression test from complaining about function parameter variations. The function simply forwards all its arguments to the main `regexp_instr` implementation without any additional processing.

The function is part of PostgreSQL's regular expression support system and provides the same functionality as `regexp_instr` but with a simplified interface that doesn't require explicit specification of regex flags.

## Parameters / Member Variables
- `fcinfo`: Standard PostgreSQL function call information structure containing all function arguments

## Dependencies
- Functions called/Symbols referenced:
  - [regexp_instr](regexp_instr.md)
- Called from (representative examples):
  - (No direct references found in the codebase)

## Notes and Other Information
- This function exists primarily for testing compatibility purposes
- It's a direct passthrough to the main regexp_instr function
- The function signature follows PostgreSQL's V1 calling convention using PG_FUNCTION_ARGS
- Located in src/backend/utils/adt/regexp.c:1266-1272