# regexp_instr_no_subexpr

## Location
src/backend/utils/adt/regexp.c: 1273 - 1282

## Overview
A PostgreSQL wrapper function that provides the regexp_instr functionality without requiring subexpression parameter to maintain compatibility with the opr_sanity regression test.

## Definition
```c
Datum regexp_instr_no_subexpr(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as another thin wrapper around the main `regexp_instr` function. Similar to `regexp_instr_no_flags`, it was created specifically to keep the opr_sanity regression test from complaining about function parameter variations. The function simply forwards all its arguments to the main `regexp_instr` implementation without any additional processing.

This variant provides the same regexp_instr functionality but with a simplified interface that doesn't require explicit specification of subexpression parameters, making it easier to use for basic pattern matching scenarios.

## Parameters / Member Variables
- `fcinfo`: Standard PostgreSQL function call information structure containing all function arguments

## Dependencies
- Functions called/Symbols referenced:
  - regexp_instr
- Called from (representative examples):
  - (No direct references found in the codebase)

## Notes and Other Information
- This function exists primarily for testing compatibility purposes
- It's a direct passthrough to the main regexp_instr function
- The function signature follows PostgreSQL's V1 calling convention using PG_FUNCTION_ARGS
- Located in src/backend/utils/adt/regexp.c:1273-1282
- This is one of several wrapper functions that provide simplified interfaces to the main regexp_instr functionality