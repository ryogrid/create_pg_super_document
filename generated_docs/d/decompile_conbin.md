# decompile_conbin

## Location
src/backend/commands/tablecmds.c: 15842 - 15866

## Overview
decompile_conbin is a static utility function that converts the binary representation of a check constraint expression back into its source text form.

## Definition


## Detailed Description
This function takes a pg_constraint tuple containing a check constraint and extracts the constraint expression from its binary form (stored in the conbin column) back to readable source text. It uses the pg_get_expr system function to perform the decompilation, which requires both the binary expression data and the relation OID for proper context resolution. The function is essential for constraint comparison operations where the textual representation of constraints needs to be analyzed.

The function performs these steps:
1. Extracts the Form_pg_constraint structure from the heap tuple
2. Retrieves the conbin attribute (binary constraint expression) from the tuple
3. Validates that conbin is not null (throws error if it is)
4. Calls pg_get_expr system function to decompile the binary expression to text
5. Converts the result to a C string and returns it

## Parameters / Member Variables
- : HeapTuple containing the pg_constraint row with the constraint information
- : TupleDesc describing the structure of the pg_constraint tuple

## Dependencies
- Functions called/Symbols referenced:
  - GETSTRUCT (macro)
  - heap_getattr
  - elog
  - DirectFunctionCall2
  - pg_get_expr
  - ObjectIdGetDatum
  - TextDatumGetCString
  - Form_pg_constraint
- Called from (representative examples):
  - constraints_equivalent (called twice)

## Notes and Other Information
- The function assumes the input tuple is from pg_constraint catalog
- Returns allocated memory that the caller is responsible for freeing
- The pg_get_expr function requires the relation OID (con->conrelid) to properly resolve column references in the expression
- Will throw an ERROR if the conbin field is unexpectedly null, indicating a corrupted constraint entry
- This function is primarily used during constraint comparison operations to determine if two constraints are equivalent