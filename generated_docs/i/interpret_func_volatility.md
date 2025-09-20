# interpret_func_volatility

## Location
[src/backend/commands/functioncmds.c:602-619](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/functioncmds.c#L602-L619)

## Overview
Converts string-based volatility specifications from CREATE FUNCTION or ALTER FUNCTION statements into the corresponding internal character constants used by PostgreSQL's catalog system.

## Definition

```c
static char
interpret_func_volatility(DefElem *defel)
```
## Detailed Description
This function takes a DefElem containing a volatility specification and translates the string value ("immutable", "stable", or "volatile") into the appropriate PostgreSQL internal constant. The function provides a clean interface for converting user-facing volatility keywords into the system catalog representation. If an invalid volatility value is provided, the function raises an error with the problematic value in the error message.

## Parameters / Member Variables
- : DefElem containing the volatility specification with a string argument

## Dependencies  
- Functions called/Symbols referenced:
  - strVal: Extracts string value from the DefElem argument
  - strcmp: Compares input string with known volatility values
  - elog: Reports errors for invalid volatility specifications
  - PROVOLATILE_IMMUTABLE: Constant for immutable functions
  - PROVOLATILE_STABLE: Constant for stable functions
  - PROVOLATILE_VOLATILE: Constant for volatile functions
- Called from (representative examples):
  - [compute_function_attributes](../c/compute_function_attributes.md): During function creation attribute processing
  - [AlterFunction](../A/AlterFunction.md): During function alteration attribute processing

## Notes and Other Information
- Returns character constants defined in PostgreSQL's catalog system (likely in pg_proc.h)
- IMMUTABLE functions cannot change within a single scan and can be pre-evaluated
- STABLE functions cannot change within a single statement but may change between statements
- VOLATILE functions may change at any time and cannot be optimized
- The function uses case-sensitive string comparisons for volatility values
- Invalid volatility specifications result in ERROR-level logging, terminating the current transaction
- The return value is used to populate the 'provolatile' column in the pg_proc system catalog