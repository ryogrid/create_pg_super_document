# function_selectivity

## Location
[src/backend/optimizer/util/plancat.c:2027-2088](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/plancat.c#L2027-L2088)

## Overview
Returns the selectivity estimate for a specified boolean function clause by executing registered procedures stored in pg_proc through the function manager.

## Definition


## Detailed Description
The function_selectivity function is a core component of PostgreSQL's query optimizer that estimates how selective a boolean function will be when used as a filter condition. It works by calling support functions registered in the pg_proc system catalog for specific functions.

The function first attempts to locate a support function for the given function ID using get_func_support(). If a support function exists, it creates a SupportRequestSelectivity structure containing all the necessary context information and calls the support function via OidFunctionCall1(). The support function can then provide a custom selectivity estimate based on its understanding of the function's behavior.

If no support function is available, the function falls back to PostgreSQL's historical default estimate of 0.3333333 (1/3), which has been used since 1992. This default represents a conservative estimate that assumes the function will filter out approximately two-thirds of the input rows.

## Parameters / Member Variables
- : PlannerInfo structure containing planning context and statistics
- : OID of the function for which to estimate selectivity
- : List of arguments passed to the function
- : OID of the input collation for the function
- : Boolean indicating whether this is part of a join condition
- : Relation ID if the function references a specific relation, 0 otherwise
- : Type of join operation (INNER, LEFT, RIGHT, FULL, etc.)
- : Special join information structure for complex join scenarios

## Dependencies
- Functions called/Symbols referenced:
  - [get_func_support](../g/get_func_support.md)
  - OidFunctionCall1
  - SupportRequestSelectivity
  - JoinType
  - [SpecialJoinInfo](../S/SpecialJoinInfo.md)
  - RegProcedure
- Called from (representative examples):
  - [clause_selectivity_ext](../c/clause_selectivity_ext.md)

## Notes and Other Information
The function validates that the returned selectivity value is between 0.0 and 1.0, throwing an error if the support function returns an invalid value. The historical default of 0.3333333 reflects PostgreSQL's conservative approach to estimation when specific knowledge about a function's behavior is unavailable. This mechanism allows function authors to provide custom selectivity estimation logic for their functions, improving query optimization accuracy.