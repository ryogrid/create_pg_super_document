# check_transform_function

## Location
src/backend/commands/functioncmds.c: 1784 - 1813

## Overview
Validates that a function meets the requirements to serve as a transform function for procedural languages.

## Definition


## Detailed Description
check_transform_function is a static helper function that validates whether a function can be used as a transform function in Postgreural language transforms. Transform functions are used to convert between SQL data types and procedural language-specific representations.

The function enforces several strict requirements:
1. The function must not be volatile (stability requirement for data conversion)
2. The function must be a normal function, not a procedure or aggregate
3. The function must not return a set (single value conversion only)
4. The function must take exactly one argument
5. The single argument must be of type 'internal' (for PLs to pass internal data structures)

These constraints ensure transform functions can reliably convert data between SQL types and procedural language types without side effects.

## Parameters / Member Variables
- : Form_pg_proc structure containing the function's catalog information from pg_proc

## Dependencies
- Functions called/Symbols referenced:
  - ereport
  - errcode
  - errmsg
  - PROVOLATILE_VOLATILE
  - PROKIND_FUNCTION
  - INTERNALOID
- Called from (representative examples):
  - CreateTransform (functioncmds.c:1892, 1917)

## Notes and Other Information
- This is a static function used internally within functioncmds.c
- Specifically designed for validating transform functions used in CREATE TRANSFORM statements
- The 'internal' argument type requirement allows procedural languages to pass opaque data structures
- All validation failures result in ERROR reports with specific error codes and messages
- Transform functions are critical for procedural language integration with PostgreSQL's type system