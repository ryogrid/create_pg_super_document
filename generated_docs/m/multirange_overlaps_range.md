# multirange_overlaps_range

## Location
src/backend/utils/adt/multirangetypes.c: 1948 - 1959

## Overview
Implements the overlap operator (&& operator) that tests whether any range in a multirange overlaps with a single range, providing the commutative counterpart to range_overlaps_multirange.

## Definition


## Detailed Description
This function provides the SQL-callable interface for testing overlap between a multirange and a range. It's the commutative version of , accepting the arguments in reverse order (multirange first, then range). The function extracts a multirange and a range from the function arguments, retrieves the appropriate type cache for range operations, and delegates to the same internal implementation .

This function is essential for providing complete operator coverage in SQL, allowing users to write overlap conditions in either order (multirange && range or range && multirange) while maintaining consistent semantics. The underlying logic remains identical regardless of argument order.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - : Multirange value ()
  - : Single range value ()

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts multirange argument from function context
  - : Extracts range argument from function context  
  - : Gets the OID of the multirange type
  - : Retrieves type cache entry for the range type
  - : Performs the actual overlap testing logic (same as range_overlaps_multirange)
  - : Returns boolean result as a Datum
- Called from (representative examples):
  - SQL overlap operations between multiranges and ranges (&& operator)
  - Spatial and temporal query operations  
  - Index-based range filtering operations

## Notes and Other Information
- This is the external interface for multirange-range overlap testing, callable from SQL
- Provides commutative behavior with  for operator completeness
- The function assumes compatible multirange and range types (enforced by PostgreSQL's type system)
- Empty ranges and multiranges never overlap with anything, following PostgreSQL range semantics
- Uses the same internal implementation as , ensuring consistent behavior
- The argument order difference is purely for SQL syntax convenience and operator symmetry
- Located in 