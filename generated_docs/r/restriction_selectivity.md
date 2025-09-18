# restriction_selectivity

## Location
src/backend/optimizer/util/plancat.c: 1947 - 1985

## Overview
Calculates the selectivity of a restriction operator clause by invoking the operator's registered selectivity estimation procedure.

## Definition


## Detailed Description
This function computes the selectivity estimate for a restriction clause (WHERE clause condition) by calling the selectivity estimation function associated with the specified operator. Selectivity represents the fraction of rows that are expected to satisfy the condition, ranging from 0.0 (no rows) to 1.0 (all rows).

The function retrieves the operator's restriction selectivity procedure () from the system catalog and invokes it through the function manager. If no selectivity function is registered for the operator, it defaults to a conservative estimate of 0.5 (50% selectivity).

The selectivity function receives context information including the planner state, operator ID, argument list, collation, and variable relation ID to make informed estimates based on statistics and operator semantics.

## Parameters / Member Variables
- : PlannerInfo containing global planner state and statistics information
- : Object ID of the operator for which to estimate selectivity
- : List of arguments (operands) to the operator clause
- : Collation ID for string comparison operations
- : Relation ID of the variable being restricted, or 0 if not applicable

## Dependencies
- Functions called/Symbols referenced:
  - [get_oprrest](../g/get_oprrest.md) (retrieves operator's restriction selectivity function)
  - [OidFunctionCall4Coll](../O/OidFunctionCall4Coll.md) (invokes the selectivity function with collation support)
  - [DatumGetFloat8](../D/DatumGetFloat8.md) (converts function result to float8)
  - RegProcedure (procedure identifier type)
  - [PointerGetDatum](../P/PointerGetDatum.md), ObjectIdGetDatum, Int32GetDatum (datum conversion functions)

- Called from (representative examples):
  - [clause_selectivity_ext](../c/clause_selectivity_ext.md) (src/backend/optimizer/path/clausesel.c:848)
  - [rowcomparesel](rowcomparesel.md) (src/backend/utils/adt/selfuncs.c:2260)
  - [test_support_func](../t/test_support_func.md) (src/test/regress/regress.c:1051)

## Notes and Other Information
- Returns default selectivity of 0.5 when no  procedure is registered
- Validates that returned selectivity is within valid range [0.0, 1.0]
- Essential component of the query optimizer's cost estimation system
- Different operators have specialized selectivity functions (e.g., eqsel for equality, scalarltsel for less-than)
- Used extensively in path cost calculations and join ordering decisions
- Part of PostgreSQL's extensible selectivity estimation framework
- Location: src/backend/optimizer/util/plancat.c:1947-1985