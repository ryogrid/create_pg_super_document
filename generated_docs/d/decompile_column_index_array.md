# decompile_column_index_array

## Location
[src/backend/utils/adt/ruleutils.c:2577-2628](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L2577-L2628)

## Overview
This utility function converts an array of column indices into a comma-separated list of quoted column names for a specified relation.

## Definition

```c
struct_array_builtin(DatumGetArrayTypeP(column_index_array), INT2OID,
							  &keys, NULL, &nKeys);
```
## Detailed Description
decompile_column_index_array is a helper function that takes a PostgreSQL array containing column indices (stored as int16 values) and converts them into a human-readable, comma-separated list of column names. The function looks up each column index in the specified relation's attribute information and appends the properly quoted column names to the provided string buffer. This function is essential for constraint definition generation where column lists need to be displayed in readable SQL format.

The function handles the formatting details such as proper comma separation and SQL identifier quoting to ensure the output is valid SQL syntax. It returns the count of columns processed, which can be useful for callers that need to know how many key columns were in the original array.

## Parameters / Member Variables
-  (Datum): PostgreSQL array containing int16 column indices (attribute numbers)
-  (Oid): Object identifier of the relation whose column names should be looked up
-  (StringInfo): String buffer where the comma-separated column name list will be appended

## Dependencies
- Functions called/Symbols referenced:
  - [deconstruct_array_builtin](deconstruct_array_builtin.md) (PostgreSQL array deconstruction utility)
  - DatumGetArrayTypeP (macro for extracting array from Datum)
  - [get_attname](../g/get_attname.md) (system function to retrieve column name by relation OID and attribute number)
  - [DatumGetInt16](../D/DatumGetInt16.md) (macro for extracting int16 from Datum)
  - [quote_identifier](../q/quote_identifier.md) (utility function for properly quoting SQL identifiers)
  - [appendStringInfoString](../a/appendStringInfoString.md)/appendStringInfo (string buffer manipulation functions)
- Called from (representative examples):
  - [pg_get_constraintdef_worker](../p/pg_get_constraintdef_worker.md) (multiple times for different constraint types)
  - Used in foreign key constraint generation for both referencing and referenced column lists
  - Used in primary key and unique constraint generation for key column lists

## Notes and Other Information
- This is a static (internal) function not exposed outside ruleutils.c
- Returns the number of columns processed, which can be useful for determining key vs. included columns in unique constraints
- Properly handles SQL identifier quoting to ensure column names with special characters are correctly escaped
- The function assumes the input array contains valid attribute numbers for the specified relation
- Formats output with proper comma separation - no comma before the first column, commas with spaces before subsequent columns
- Located in src/backend/utils/adt/ruleutils.c:2577-2628
- Essential component of PostgreSQL's constraint definition reconstruction system