# build_regtype_array

## Location
src/backend/commands/prepare.c: 746 - 759

## Overview
A utility function that converts a C array of PostgreSQL type OIDs into a PostgreSQL array of regtype values for SQL-level consumption.

## Definition
```c
static Datum build_regtype_array(Oid *param_types, int num_params)
```

## Detailed Description
This static utility function provides a bridge between C-level type representation and SQL-level type arrays. It takes an array of PostgreSQL type OIDs (internal type identifiers) and constructs a corresponding PostgreSQL array of regtype values, which can be displayed in a human-readable format in SQL queries.

The function handles empty arrays gracefully by returning a zero-element array rather than NULL, ensuring consistent behavior in SQL contexts. The resulting array uses PostgreSQL's regtype pseudo-type, which automatically provides readable type names when displayed.

## Parameters / Member Variables
- `param_types`: C array of Oid values representing PostgreSQL data types
- `num_params`: Number of elements in the param_types array

## Dependencies
- Functions called/Symbols referenced:
  - palloc_array
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - [construct_array_builtin](../c/construct_array_builtin.md)
  - [PointerGetDatum](../P/PointerGetDatum.md)
- Constants used:
  - REGTYPEOID
- Data structures used:
  - Datum
  - [ArrayType](../A/ArrayType.md)
  - Oid
- Called from (representative examples):
  - [pg_prepared_statement](../p/pg_prepared_statement.md)

## Notes and Other Information
- This is a static function, only accessible within the prepare.c compilation unit
- Returns a zero-element array for empty input rather than NULL, maintaining SQL array semantics
- Uses PostgreSQL's built-in array construction facilities for proper memory management
- The regtype pseudo-type provides automatic OID-to-name translation when values are displayed
- Primarily used in system view functions to present type information in a user-friendly format
- Memory allocation uses palloc_array for proper PostgreSQL memory context handling
- The function assumes all input OIDs are valid PostgreSQL type identifiers