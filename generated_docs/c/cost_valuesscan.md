# cost_valuesscan

## Location
[src/backend/optimizer/path/costsize.c:1648-1697](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L1648-L1697)

## Overview
Determines and returns the cost of scanning a VALUES RTE (Range Table Entry), calculating startup and run costs for evaluating a VALUES clause in a query.

## Definition


## Detailed Description
This function calculates the execution cost for scanning a VALUES clause, which represents a literal list of rows in SQL (e.g., VALUES (1,'a'), (2,'b')). The cost estimation considers:

1. **Row estimation**: Uses parameterized path information if available, otherwise uses base relation row estimates
2. **CPU costs per tuple**: Estimates one operator evaluation per list item plus standard tuple processing costs  
3. **Qualification costs**: Accounts for any restriction qualifiers that need to be evaluated
4. **Target list evaluation**: Includes costs for computing the output columns

The function follows PostgreSQL's standard cost model by separating startup costs (one-time initialization) from run costs (proportional to number of tuples processed).

## Parameters / Member Variables
- : The Path node to store the calculated costs in (startup_cost and total_cost fields are set)
- : PlannerInfo structure containing global planning information and cost parameters
- : RelOptInfo representing the VALUES relation being scanned (must have rtekind == RTE_VALUES)
- : ParamPathInfo for parameterized paths, or NULL for non-parameterized scans

## Dependencies
- Functions called/Symbols referenced:
  - [get_restriction_qual_cost](../g/get_restriction_qual_cost.md) (calculates cost of applying WHERE clause conditions)
  - cpu_operator_cost (global cost parameter)
  - cpu_tuple_cost (global cost parameter)
- Types referenced:
  - [ParamPathInfo](../P/ParamPathInfo.md) (parameterized path information)
  - Cost (cost calculation type)
  - QualCost (qualification cost structure)
  - RTE_VALUES (enum value for VALUES range table entries)
- Called from:
  - [create_valuesscan_path](create_valuesscan_path.md) (in pathnode.c:2113)

## Notes and Other Information
- The function includes an assertion that the relation must be a VALUES RTE (rtekind == RTE_VALUES)
- Cost estimation for list evaluation is acknowledged as "probably pretty bogus" in the code comments, using a simplified model of one operator cost per tuple
- The function properly handles both parameterized and non-parameterized paths
- Target list evaluation costs are applied per output row rather than per tuple scanned, which is important for queries that filter the VALUES list