# pg_mcv_list_recv

## Location
[src/backend/statistics/mcv.c:1507-1522](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/mcv.c#L1507-L1522)

## Overview
Binary input routine for the pg_mcv_list data type that explicitly disallows binary input operations, maintaining the type's read-only nature.

## Definition


## Detailed Description
This function serves as the required binary input routine for PostgreSQL's pg_mcv_list data type, but like its text input counterpart , it deliberately prevents any external input operations. The function immediately raises an error when called, ensuring that MCV lists cannot be created or modified through binary input mechanisms.

This design choice reinforces the principle that pg_mcv_list values should only be generated internally by PostgreSQL's statistics collection system. By blocking both text and binary input routines, the system ensures that the complex statistical data structures cannot be corrupted or incorrectly constructed by external operations.

The function follows the same pattern as  but handles the binary input path of PostgreSQL's type system.

## Parameters / Member Variables
- Uses standard PostgreSQL function call interface via  macro
- No actual parameters are processed since the function rejects all input

## Dependencies
- Functions called/Symbols referenced:
  -  - Error reporting mechanism
  -  - Macro for returning void (unreachable due to error)
  - Error codes: 

- Called from (representative examples):
  - PostgreSQL type system during binary data deserialization
  - Protocol-level operations that would attempt binary input
  - Replication or data import processes

## Notes and Other Information
- Companion to  that blocks text input; together they prevent all external input
- Part of PostgreSQL's type system infrastructure ensuring data integrity
- The function always raises an error and never returns normally  
- This pattern is used for PostgreSQL data types that should only be created internally
- Prevents accidental corruption of statistical data through external manipulation
- Essential for maintaining the integrity of extended statistics in PostgreSQL's query planner