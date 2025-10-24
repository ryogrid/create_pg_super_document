# pg_mcv_list_in

## Location
[src/backend/statistics/mcv.c:1472-1497](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/mcv.c#L1472-L1497)

## Overview
Input routine for the pg_mcv_list data type that explicitly disallows text input since MCV lists are stored in binary format only.

## Definition

```c
Datum
pg_mcv_list_in(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the required input routine for the pg_mcv_list PostgreSQL data type, but it intentionally prevents any text-based input operations. The pg_mcv_list type is designed to store Most Common Values statistics data exclusively in binary format, as text parsing of such complex statistical structures would be impractical and error-prone.

The function immediately raises an error indicating that the pg_mcv_list type cannot accept values through normal SQL input mechanisms. This design choice enforces that MCV lists can only be created and manipulated through internal PostgreSQL statistics collection processes.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  -  - Error reporting mechanism
  -  - Macro for returning void (unreachable due to error)
  - Error codes: 

- Called from (representative examples):
  - PostgreSQL type system when attempting text input conversion
  - SQL operations that would require parsing pg_mcv_list from text

## Notes and Other Information
- Part of PostgreSQL's type system infrastructure for the pg_mcv_list data type
- The function always raises an error and never returns normally
- This design pattern is common for binary-only PostgreSQL data types
- Ensures data integrity by preventing manual construction of complex statistical structures
- The pg_mcv_list type can only be populated through internal statistics collection mechanisms

## Simplified Source

```c
Datum pg_mcv_list_in(PG_FUNCTION_ARGS) {
    // Reject text input for pg_mcv_list type
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("cannot accept a value of type %s", "pg_mcv_list")));

    PG_RETURN_VOID();  // Never reached
}
```