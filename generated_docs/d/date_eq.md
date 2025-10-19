# date_eq

## Location
[src/backend/utils/adt/date.c:383-391](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L383-L391)

## Overview
date_eq is a comparison function that tests whether two date values are equal, implementing the equality operator (=) for the DATE data type.

## Definition
```c
Datum date_eq(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the equality comparison operator for PostgreSQL's DATE data type. It takes two date arguments through the PostgreSQL function call interface and performs a simple equality comparison of their internal representations. The function follows PostgreSQL's standard function interface pattern using PG_FUNCTION_ARGS and returns a boolean result indicating whether the two dates are equal.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument interface that provides:
  - Argument 0: First DateADT value to compare
  - Argument 1: Second DateADT value to compare

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_DATEADT (macro for extracting DateADT arguments)
  - PG_RETURN_BOOL (macro for returning boolean results)
- Types used:
  - DateADT
  - Datum
- Called from (representative examples):
  - Used internally by PostgreSQL's operator system for DATE = DATE operations

## Notes and Other Information
- Part of the comparison function family for dates (along with date_ne, date_lt, etc.)
- Performs direct integer comparison since DateADT is internally represented as days since epoch
- Used by PostgreSQL's operator system to implement the '=' operator for DATE data types
- Returns SQL boolean true if dates are equal, false otherwise
- Function follows PostgreSQL's V1 calling convention

## Simplified Source

```c
Datum date_eq(PG_FUNCTION_ARGS) {
    DateADT dateVal1 = PG_GETARG_DATEADT(0);
    DateADT dateVal2 = PG_GETARG_DATEADT(1);

    PG_RETURN_BOOL(dateVal1 == dateVal2);
}
```