# amvalidate

## Location
[src/backend/access/index/amapi.c:114-143](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/index/amapi.c#L114-L143)

## Overview
amvalidate is a PostgreSQL function that validates operator classes by calling the appropriate index access method's validation routine.

## Definition
Datum amvalidate(PG_FUNCTION_ARGS)

## Detailed Description
This function serves as a SQL-callable interface for validating operator classes in PostgreSQL. It acts as a dispatcher that:

1. Extracts the operator class OID from the function arguments
2. Looks up the operator class in the pg_opclass system catalog
3. Identifies the associated index access method
4. Retrieves the IndexAmRoutine for that access method
5. Calls the access method's specific validation function
6. Returns the validation result as a boolean

The function ensures that operator classes are properly structured and contain all necessary operators and support functions required by their associated index access method. This is crucial for maintaining index functionality and preventing runtime errors.

## Parameters / Member Variables
- Function takes PG_FUNCTION_ARGS (standard PostgreSQL function interface)
- : The OID of the operator class to validate (extracted from args[0])

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - [GetIndexAmRoutineByAmId](../G/GetIndexAmRoutineByAmId.md)
  - [pfree](../p/pfree.md)
  - PG_RETURN_BOOL
  - Form_pg_opclass
  - [IndexAmRoutine](../I/IndexAmRoutine.md)
- Called from (representative examples):
  - Various index access method handlers (brinhandler, ginhandler, gisthandler, etc.)
  - SQL interface as a system function

## Notes and Other Information
- Located in src/backend/access/index/amapi.c:114-143
- This is a PostgreSQL function that can be called from SQL as part of system maintenance
- Each index access method must provide its own amvalidate implementation in the IndexAmRoutine struct
- The function will error if the access method doesn't provide a validation routine
- Validation typically checks operator class completeness, operator signatures, and support function availability
- Essential for database consistency and preventing malformed operator class definitions