# oideq

## Location
[src/backend/utils/adt/oid.c:272-280](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/oid.c#L272-L280)

## Overview
A PostgreSQL function that tests equality between two Oid (Object Identifier) values, returning a boolean result indicating whether the two Oids are equal.

## Definition
```c
Datum oideq(PG_FUNCTION_ARGS)
```

## Detailed Description
The oideq function is a PostgreSQL system function that implements the equality operator (=) for the Oid data type. It follows the standard PostgreSQL function calling convention using PG_FUNCTION_ARGS to receive parameters and returns a Datum. The function extracts two Oid arguments using PG_GETARG_OID macros and performs a simple equality comparison using the == operator. The result is returned as a boolean value using PG_RETURN_BOOL. This function is typically invoked through SQL queries when comparing Oid values with the = operator.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `arg1`: First Oid value (extracted via PG_GETARG_OID(0))
  - `arg2`: Second Oid value (extracted via PG_GETARG_OID(1))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID (macro)
  - PG_RETURN_BOOL (macro)
- Called from (representative examples):
  - No direct references found (called through PostgreSQL function call mechanism)

## Notes and Other Information
This function is part of PostgreSQL's operator infrastructure and is typically not called directly in C code but rather invoked through SQL expressions using the = operator on Oid values. The function follows PostgreSQL's standard function calling convention and is registered in the system catalogs as the implementation for Oid equality comparison.

## Simplified Source

```c
Datum oideq(PG_FUNCTION_ARGS) {
    // Extract the two OID arguments
    Oid arg1 = PG_GETARG_OID(0);
    Oid arg2 = PG_GETARG_OID(1);

    // Compare for equality and return boolean result
    PG_RETURN_BOOL(arg1 == arg2);
}
```