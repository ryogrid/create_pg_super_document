# int8dec_any

## Location
src/backend/utils/adt/int8.c: 816 - 825

## Overview
A wrapper function that provides an alternative entry point to the int8dec function for decrementing 64-bit signed integers by 1.

## Definition
Datum int8dec_any(PG_FUNCTION_ARGS)

## Detailed Description
int8dec_any is a simple wrapper function that delegates to the int8dec function. It serves as an alternative function entry point, likely used in contexts where a different function signature or calling convention is needed while maintaining the same core decrementing functionality. The function takes PostgreSQL function call information and passes it directly to int8dec for processing.

## Parameters / Member Variables
- PG_FUNCTION_ARGS: Standard PostgreSQL macro containing function call information including arguments and context

## Dependencies
- Functions called/Symbols referenced:
  - [int8dec](int8dec.md)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
This function appears to be a compatibility or alternative interface wrapper around int8dec. The suffix suggests it may be used in contexts where type flexibility or generic handling is required. The function is defined in src/backend/utils/adt/int8.c:816-825.