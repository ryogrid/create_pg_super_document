# neqjoinsel

## Location
[src/backend/utils/adt/selfuncs.c:2823-2900](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L2823-L2900)

## Overview
Estimates the selectivity of a "!=" (not equal) operator in join conditions, returning the fraction of row combinations that satisfy the inequality join predicate.

## Definition

```c
Datum
neqjoinsel(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function calculates join selectivity for inequality ("!=") operations by computing the complement of equality selectivity. It handles different join types with specialized logic:

For semi-joins and anti-joins, it assumes multiple distinct values exist in the RHS relation, making the selectivity equal to  where nullfrac represents the fraction of NULL values.

For regular joins, it finds the associated equality operator (negator of the != operator) and computes  to determine the inequality selectivity. This approach leverages existing equality selectivity estimation infrastructure.

The function is designed to work within PostgreSQL's cost-based query optimizer to help determine optimal join strategies.

## Parameters / Member Variables
- : PlannerInfo pointer containing query planning context and statistics
- : OID of the inequality operator being evaluated  
- : List of arguments to the join operator
- : Type of join operation (inner, left, right, semi, anti, etc.)
- : SpecialJoinInfo structure with additional join context information
- : Collation information for the operation

## Dependencies
- Functions called/Symbols referenced:
  - [get_join_variables](../g/get_join_variables.md)
  - [get_negator](../g/get_negator.md)
  - [eqjoinsel](../e/eqjoinsel.md)
  - [DirectFunctionCall5Coll](../D/DirectFunctionCall5Coll.md)
  - ReleaseVariableStats
  - [DatumGetFloat8](../D/DatumGetFloat8.md)
  - DEFAULT_EQ_SEL
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Uses PostgreSQL's function manager interface (PG_FUNCTION_ARGS, PG_RETURN_FLOAT8)
- Implements different strategies for semi/anti-joins versus regular joins
- Falls back to DEFAULT_EQ_SEL when the negator operator cannot be found
- Part of PostgreSQL's selectivity estimation framework in src/backend/utils/adt/selfuncs.c
- Location: src/backend/utils/adt/selfuncs.c:2823-2900