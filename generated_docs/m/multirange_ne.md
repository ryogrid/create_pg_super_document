# multirange_ne

## Location
src/backend/utils/adt/multirangetypes.c: 1923 - 1935

## Overview
Implements the inequality operator (<> or !=) for multirange types, comparing two multirange values to determine if they contain different sets of ranges or different ordering.

## Definition


## Detailed Description
This function serves as the SQL-callable wrapper for multirange inequality comparison. It mirrors the structure of  but delegates to  to perform the actual comparison logic. The function extracts two multirange arguments from the function call context, retrieves the appropriate type cache entry for the multirange type, and returns the boolean result of the inequality comparison.

The inequality comparison will return true if the multiranges differ in any way: different number of ranges, different range bounds, or any structural differences. This is the complement of the equality operation and is essential for SQL WHERE clauses, joins, and other conditional operations involving multirange comparisons.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - : First multirange value ()
  - : Second multirange value ()

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts multirange arguments from function context
  - : Gets the OID of the multirange type
  - : Retrieves type cache entry for the multirange type
  - : Performs the actual inequality comparison logic
  - : Returns boolean result as a Datum
- Called from (representative examples):
  - SQL inequality operations on multirange types (<> or != operators)
  - Internal range comparison operations

## Notes and Other Information
- This is the external interface for multirange inequality comparison, callable from SQL
- The function assumes both arguments are of the same multirange type (enforced by PostgreSQL's type system)
- Provides the logical complement to  for complete comparison support
- Type cache lookup is performed to access range-specific comparison functions
- The underlying logic leverages the existing equality comparison and negates the result
- Located in 