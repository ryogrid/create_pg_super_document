# AggClauseCosts

## Location
[src/include/nodes/pathnodes.h:58-63](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L58-L63)

## Overview
AggClauseCosts is a structure that contains cost statistics for aggregate function execution, including both transition costs (per input row) and finalization costs (per aggregated row).

## Definition
```c
typedef struct AggClauseCosts
{
    QualCost    transCost;        /* total per-input-row execution costs */
    QualCost    finalCost;        /* total per-aggregated-row costs */
    Size        transitionSpace;  /* space for pass-by-ref transition data */
} AggClauseCosts;
```

## Detailed Description
AggClauseCosts encapsulates the cost statistics required for accurate cost estimation of aggregate operations in PostgreSQL's query planner. The structure separates aggregate execution costs into two phases: the transition phase (processing each input row) and the finalization phase (producing the final aggregate result). This cost model reflects how PostgreSQL's aggregate functions actually work internally, where transition functions are called for each input row to update aggregate state, and finalization functions are called once per group to produce final results. The structure also tracks memory space requirements for pass-by-reference transition data, which is important for estimating memory usage in hash-based aggregation.

## Parameters / Member Variables
- `transCost`: Cost estimates for processing each input row during the transition phase, including execution costs of aggregate arguments and transition functions
- `finalCost`: Cost estimates for finalization operations performed once per aggregated group to produce final results
- `transitionSpace`: Memory space required for storing pass-by-reference transition state data

## Dependencies
- Functions called/Symbols referenced:
  - QualCost (for both transCost and finalCost members)
  - Size (for transitionSpace)
- Called from (representative examples):
  - get_agg_clause_costs (primary function for computing aggregate costs)
  - cost_agg (aggregate node costing)
  - create_agg_path (aggregate path creation)
  - create_groupingsets_path (grouping sets path creation)
  - estimate_hashagg_tablesize (hash aggregate memory estimation)

## Notes and Other Information
- Designed to be safely initialized to zero with memset, making it easy to use in various contexts
- Essential for comparing costs between different aggregation strategies (hash vs sort-based)
- Used extensively in grouping and aggregation path planning throughout the optimizer
- The separation of transition and final costs enables accurate modeling of aggregate operations with different computational profiles
- Critical for memory planning in hash-based aggregation where transition space affects hash table sizing