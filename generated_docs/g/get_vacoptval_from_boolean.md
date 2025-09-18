# get_vacoptval_from_boolean

## Location
[src/backend/commands/vacuum.c:2526-2536](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuum.c#L2526-L2536)

## Overview
A wrapper function that converts boolean values from DefElem to VacOptValue enum constants for vacuum option processing.

## Definition


## Detailed Description
This utility function serves as an adapter between the generic boolean value extraction mechanism (defGetBoolean) and the vacuum-specific option value system. It converts true/false boolean values into the corresponding VacOptValue enumeration constants (VACOPTVALUE_ENABLED/VACOPTVALUE_DISABLED), providing type safety and consistency in vacuum option handling throughout the codebase.

## Parameters / Member Variables
- : Pointer to a DefElem structure containing the boolean definition element to be converted

## Dependencies
- Functions called/Symbols referenced:
  - [defGetBoolean](../d/defGetBoolean.md)
  - VACOPTVALUE_ENABLED (enum constant)
  - VACOPTVALUE_DISABLED (enum constant)
- Called from (representative examples):
  - ExecVacuum (multiple times for different vacuum options)

## Notes and Other Information
- Function is static (internal to vacuum.c)
- Returns VacOptValue enum type instead of plain boolean
- Provides a consistent interface for converting boolean vacuum options
- Part of the vacuum option parsing infrastructure
- Simple one-line conditional expression implementation
- Used for processing various boolean vacuum options in the VACUUM command