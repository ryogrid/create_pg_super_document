# binary_coercible

## Location
[src/test/regress/regress.c:1291-1297](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/regress.c#L1291-L1297)

## Overview
A PostgreSQL regression test function that checks whether one data type can be binary-coercible to another data type without requiring an explicit cast.

## Definition

```c
Datum
binary_coercible(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a simple PostgreSQL test utility that determines if a source data type can be implicitly converted (binary coerced) to a target data type. Binary coercion is a form of implicit type conversion where the internal representation of the data remains the same, but PostgreSQL treats it as a different type. This is the most efficient form of type conversion since no data transformation is required.

The function serves as a wrapper around PostgreSQL's internal  function, making this capability accessible for testing purposes in the regression test suite.

## Parameters / Member Variables
-  (Oid): The OID of the source data type
-  (Oid): The OID of the target data type to convert to

## Dependencies
- Functions called/Symbols referenced:
  - : Extract OID parameter from function arguments
  - : Internal PostgreSQL function to check binary coercibility
  - : Return boolean result
- Called from (representative examples):
  - Referenced in  at src/test/regress/regress.c:1289

## Notes and Other Information
- Located in the regression test suite ()
- Binary coercion is typically allowed between types with identical internal representations
- Common examples include coercion between domains and their base types, or between types in the same type family
- Returns true if binary coercion is possible, false otherwise
- This is a lightweight wrapper function that exposes internal PostgreSQL type system functionality for testing

## Simplified Source

```c
Datum
binary_coercible(PG_FUNCTION_ARGS)
{
    // Extract source and target type OIDs
    Oid srctype = PG_GETARG_OID(0);
    Oid targettype = PG_GETARG_OID(1);

    // Check if source type can be binary-coerced to target type
    PG_RETURN_BOOL(IsBinaryCoercible(srctype, targettype));
}
```