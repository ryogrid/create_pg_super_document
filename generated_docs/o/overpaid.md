# overpaid

## Location
[src/test/regress/regress.c:142-162](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/regress.c#L142-L162)

## Overview
The overpaid function is a user-defined PostgreSQL function that determines whether an employee is overpaid by checking if their salary exceeds 699.

## Definition
```c
Datum overpaid(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is part of PostgreSQL's regression testing suite and serves as an example of a user-defined function that operates on tuple data. It extracts the 'salary' attribute from a given tuple (representing an employee record) and returns true if the salary is greater than 699, false otherwise. If the salary attribute is NULL, the function returns NULL. This function demonstrates how to create custom SQL functions that can be used in queries to filter or evaluate data based on specific business logic.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to:
  - First argument: HeapTupleHeader representing a database tuple/record that should contain a 'salary' attribute

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_HEAPTUPLEHEADER (macro for extracting tuple header from arguments)
  - [GetAttributeByName](../G/GetAttributeByName.md) (retrieves attribute value from tuple by name)
  - [DatumGetInt32](../D/DatumGetInt32.md) (converts Datum to 32-bit integer)
  - PG_RETURN_BOOL (returns boolean value)
  - PG_RETURN_NULL (returns NULL value)
- Data types used:
  - HeapTupleHeader (PostgreSQL's internal tuple representation)
  - Datum (PostgreSQL's generic data type)
  - int32 (32-bit signed integer)
  - [bool](../b/bool.md) (boolean type)

- Called from (representative examples):
  - [regress_lseg_construct](../r/regress_lseg_construct.md) (referenced in the same file)

## Notes and Other Information
- This function is specifically designed for regression testing and demonstrates user-defined function capabilities
- The threshold value of 699 is hardcoded and appears to be an arbitrary value chosen for testing purposes
- Handles NULL salary values gracefully by returning NULL rather than causing an error
- The function can be called from SQL queries to filter employee records based on salary criteria
- Located in src/test/regress/regress.c as part of the PostgreSQL test suite
- Serves as an example of how to write custom functions that operate on database tuples