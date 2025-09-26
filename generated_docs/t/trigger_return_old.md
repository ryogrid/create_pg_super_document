# trigger_return_old

## Location
[src/test/regress/regress.c:254-266](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/regress.c#L254-L266)

## Overview
This function is a PostgreSQL trigger function that returns the old tuple (row) that triggered the database operation, primarily used for testing trigger functionality.

## Definition
```c
Datum trigger_return_old(PG_FUNCTION_ARGS)
```

## Detailed Description
trigger_return_old is a PostgreSQL trigger function designed to return the original tuple that was involved in the triggering database operation. The function validates that it was properly called as a trigger using the CALLED_AS_TRIGGER macro, and if not, it raises an error. When called correctly, it extracts the trigger data from the function context, retrieves the old tuple (tg_trigtuple), and returns it as a Datum. This is commonly used in BEFORE triggers or in situations where you need to access the original row data before modification.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to:
  - `fcinfo->context`: Contains TriggerData when called as a trigger
  - TriggerData fields accessed:
    - `tg_trigtuple`: The old tuple that triggered the operation

## Dependencies
- Functions called/Symbols referenced:
  - [TriggerData](../T/TriggerData.md) (structure containing trigger context information)
  - CALLED_AS_TRIGGER (macro to validate trigger context)
  - elog (PostgreSQL logging/error function)
  - [PointerGetDatum](../P/PointerGetDatum.md) (converts pointer to Datum)
- Called from (representative examples):
  - [reverse_name](../r/reverse_name.md) (appears in reference context)

## Notes and Other Information
- This function is part of PostgreSQL's regression test suite for testing trigger mechanisms
- Must be called only in trigger context, otherwise raises an ERROR
- Returns the 'old' tuple, which represents the row state before the triggering operation
- Commonly used in BEFORE UPDATE or BEFORE DELETE triggers where you need to examine the original row data
- The returned tuple can be used by the trigger system to determine whether to proceed with the operation
- Located in src/test/regress/regress.c, indicating it's primarily for testing trigger functionality
- The function demonstrates proper trigger function structure and validation patterns