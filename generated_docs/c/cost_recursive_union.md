# cost_recursive_union

## Location
src/backend/optimizer/path/costsize.c: 1813 - 1883

## Overview
Determines and returns the cost and estimated output size of performing a recursive union operation, which is used in recursive Common Table Expressions (CTEs).

## Definition


## Detailed Description
This function calculates the execution cost for a recursive union operation, which is the core mechanism behind recursive CTEs in PostgreSQL. The cost estimation involves:

1. **Non-recursive term**: Uses actual cost estimates from the non-recursive (anchor) query
2. **Recursive iterations**: Makes an assumption of approximately 10 recursive iterations, multiplying the recursive term costs accordingly
3. **Tuplestore manipulation**: Adds costs for managing tuplestores that hold intermediate results between iterations
4. **Output estimation**: Combines row estimates from both terms with the iteration multiplier

The function acknowledges that the assumptions are "mighty shaky" but represents the best approximation possible given the inherent unpredictability of recursive query behavior.

## Parameters / Member Variables
- : The Path node for the recursive union to store calculated costs and row estimates
- : Path for the non-recursive (anchor) term of the recursive CTE
- : Path for the recursive term that will be executed iteratively

## Dependencies
- Functions called/Symbols referenced:
  - cpu_tuple_cost (global cost parameter for tuple processing)
  - Max (macro for maximum value comparison)
- Types referenced:
  - Cost (cost calculation type)
  - Path (query path structure)
- Called from:
  - create_recursiveunion_path (in pathnode.c:3647)

## Notes and Other Information
- Uses a hardcoded assumption of 10 recursive iterations, which the code acknowledges as a rough approximation
- The startup cost equals the non-recursive term's startup cost since that must complete before recursion begins
- Includes tuplestore manipulation costs via cpu_tuple_cost for all produced rows
- Does not account for potential spill-to-disk costs of tuplestores, assuming in-memory operation
- Sets the path width to the maximum width between non-recursive and recursive terms
- The cost model is inherently imprecise due to the unpredictable nature of recursive query convergence
- Total cost formula: nrterm_cost + (10 × rterm_cost) + (cpu_tuple_cost × total_rows)
- Represents one of the more challenging areas of PostgreSQL cost estimation due to runtime variability