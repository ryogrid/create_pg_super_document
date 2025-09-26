# eqjoinsel

## Location
[src/backend/utils/adt/selfuncs.c:2273-2437](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L2273-L2437)

## Overview
Calculates join selectivity for equality ("=") operators, serving as the core PostgreSQL function for estimating how many rows will result from equality-based joins between relations.

## Definition

```c
Datum
eqjoinsel(PG_FUNCTION_ARGS)
```
## Detailed Description
This PostgreSQL function estimates the selectivity of equality joins by analyzing statistics from both sides of the join condition. The function handles different join types (INNER, LEFT, FULL, SEMI, ANTI) and uses sophisticated statistical analysis including Most Common Values (MCVs) when available.

The estimation process involves:
1. **Variable analysis**: Extracting statistical information from both join variables using 
2. **Distinct value estimation**: Computing the number of distinct values using 
3. **MCV statistics**: Fetching and analyzing Most Common Values when security checks pass
4. **Join-type specific computation**: Delegating to specialized functions (, ) based on join type
5. **Result clamping**: Ensuring semi/anti join selectivity doesn't exceed inner join selectivity

For SEMI and ANTI joins, the function ensures logical consistency by clamping the result to not exceed what an equivalent inner join would produce.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  - : PlannerInfo structure with planner context
  - : OID of the equality operator
  - : List containing the two join arguments
  - : Type of join operation (currently unused but preserved)
  - : SpecialJoinInfo containing join metadata
  - : Collation information for the comparison

## Dependencies
- Functions called/Symbols referenced:
  - [get_join_variables](../g/get_join_variables.md)
  - [get_variable_numdistinct](../g/get_variable_numdistinct.md)
  - [get_opcode](../g/get_opcode.md)
  - [get_attstatsslot](../g/get_attstatsslot.md)
  - [statistic_proc_security_check](../s/statistic_proc_security_check.md)
  - [eqjoinsel_inner](eqjoinsel_inner.md)
  - [eqjoinsel_semi](eqjoinsel_semi.md)
  - [find_join_input_rel](../f/find_join_input_rel.md)
  - [get_commutator](../g/get_commutator.md)
  - [free_attstatsslot](../f/free_attstatsslot.md)
  - ReleaseVariableStats
  - CLAMP_PROBABILITY
- Called from (representative examples):
  - [neqjoinsel](../n/neqjoinsel.md)

## Notes and Other Information
- The function requires valid statistics tuples from both sides to effectively use MCV (Most Common Values) analysis
- Security checks are performed before accessing detailed statistics to prevent information leakage
- For SEMI/ANTI joins with reversed arguments, the function finds the commutator operator and swaps arguments appropriately
- The result is clamped to ensure semi-join selectivity never exceeds inner-join selectivity for the same conditions
- Memory management is carefully handled with proper cleanup of statistics slots and variable stats
- The function returns a float8 value representing the estimated selectivity (fraction of qualifying rows)