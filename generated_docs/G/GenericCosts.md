# GenericCosts

## Location
[src/include/utils/selfuncs.h:138-246](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/selfuncs.h#L138-L246)

## Overview
GenericCosts is a structure that holds cost estimation data for index operations, used by PostgreSQL's cost estimator functions to provide intermediate calculations and final cost estimates to the query planner.

## Definition

```c
typedef bool (*get_relation_stats_hook_type) (PlannerInfo *root,
											  RangeTblEntry *rte,
											  AttrNumber attnum,
											  VariableStatData *vardata);
```
## Detailed Description
GenericCosts serves as a comprehensive data structure for index cost estimation in PostgreSQL's query planner. It is primarily used by the genericcostestimate() function, which provides a general-purpose cost estimation framework that can be used by most index types. The structure is designed to avoid code duplication by allowing index-specific cost estimators to use genericcostestimate() as a base and then incorporate additional type-specific knowledge.

The structure contains both the final cost estimates that must be returned to the planner (startup cost, total cost, selectivity, and correlation) as well as intermediate values computed during the estimation process. This design allows index access methods to reuse the generic calculations while customizing specific aspects based on their unique characteristics.

## Parameters / Member Variables
- : Cost associated with starting an index scan (setup overhead)
- : Total cost for the entire index scan operation
- : Estimated selectivity of the index scan (fraction of tuples returned)
- : Statistical correlation between index order and table physical order
- : Estimated number of index leaf pages that will be visited during the scan
- : Estimated number of index leaf tuples that will be examined
- : The relevant random_page_cost setting for cost calculations
- : Number of index scans required for ScalarArrayOp expressions (typically 1, but can be higher)

## Dependencies
- Functions called/Symbols referenced:
  - Cost (cost estimation type)
  - Selectivity (selectivity estimation type)
  - Used by genericcostestimate function
  - Used by various index-specific cost estimators

- Called from (representative examples):
  - genericcostestimate
  - btcostestimate (B-tree index cost estimation)
  - hashcostestimate (hash index cost estimation) 
  - gistcostestimate (GiST index cost estimation)
  - spgcostestimate (SP-GiST index cost estimation)

## Notes and Other Information
- Callers should initialize all fields to zero before calling genericcostestimate()
- The numIndexTuples field can be pre-set by callers if they have better estimates than the default
- The num_sa_scans field can be set to values >= 1 for index AMs that may perform multiple primitive scans per ScalarArrayOp
- This structure enables code reuse across different index access methods while allowing customization
- The design separates required outputs (for the planner) from intermediate calculations (for reuse and debugging)