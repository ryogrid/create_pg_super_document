# btrim1

## Location
src/backend/utils/adt/oracle_compat.c: 362 - 377

## Overview
The btrim1 function is a specialized version of btrim that removes whitespace characters from both the front and back of a text string.

## Definition
```c
Datum btrim1(PG_FUNCTION_ARGS)
```

## Detailed Description
btrim1 is a PostgreSQL built-in function that provides a simplified interface for the common use case of trimming whitespace from text strings. Unlike the general btrim function which accepts a custom set of characters to remove, btrim1 has a fixed character set consisting only of space characters (' '). It removes spaces from both the beginning and end of the input string, making it equivalent to calling btrim(string, ' ').

## Parameters / Member Variables
- `string` (text): The input text string to be trimmed of whitespace characters

## Dependencies
- Functions called/Symbols referenced:
  - dotrim (core trimming logic function)
  - PG_RETURN_TEXT_P (PostgreSQL macro for returning text values)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's Oracle compatibility layer
- Located in src/backend/utils/adt/oracle_compat.c:362-377
- Uses the dotrim helper function with a hardcoded space character set (" ", 1)
- Performs bidirectional trimming (both front and back enabled with true, true parameters)
- Provides a more efficient alternative to btrim when only whitespace trimming is needed