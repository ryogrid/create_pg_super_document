# preprocess_aggref

## Location
[src/backend/optimizer/prep/prepagg.c:116-343](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepagg.c#L116-L343)

## Overview
Preprocesses a single Aggref (aggregate reference) node by resolving its transition type, finding compatible aggregates for optimization, and setting up aggregate and transition state information.

## Definition
```c
static void preprocess_aggref(Aggref *aggref, PlannerInfo *root)
```

## Detailed Description
This function performs detailed preprocessing of an individual aggregate function reference. It is the core workhorse function that:

1. **Fetches aggregate metadata**: Retrieves aggregate function information from pg_aggregate system catalog
2. **Resolves polymorphic types**: Determines the actual transition type for polymorphic aggregates based on input arguments  
3. **Determines shareability**: Checks if the aggregate's transition state can be shared with other aggregates based on final function modify behavior
4. **Finds compatible aggregates**: Searches for identical aggregate calls that can reuse the same final result
5. **Finds compatible transitions**: Searches for different aggregates that can share the same transition state
6. **Creates new aggregate/transition info**: If no compatible aggregates are found, creates new AggInfo and AggTransInfo structures
7. **Handles partial aggregation**: Determines feasibility of partial aggregation based on combine functions and serialization capabilities
8. **Updates Aggref fields**: Fills in aggno, aggtransno, and aggtranstype fields in the Aggref node

The function performs critical optimizations by detecting:
- **Identical aggregates**: Same function calls that can share both transition state and final values
- **Compatible transitions**: Different functions that can share transition state but need separate final processing

Special handling is included for:
- Polymorphic aggregate functions
- Read-write final functions that cannot share state
- INTERNAL transition types requiring serialization/deserialization
- Array aggregates with special serialization requirements
- Ordered aggregates that defeat partial aggregation

## Parameters / Member Variables
- : Aggref node representing the aggregate function reference to be processed
- : PlannerInfo structure containing planner context, including agginfos and aggtransinfos lists

## Dependencies
- Functions called/Symbols referenced:
  - [get_aggregate_argtypes](../g/get_aggregate_argtypes.md)
  - [resolve_aggregate_transtype](../r/resolve_aggregate_transtype.md)  
  - [find_compatible_agg](../f/find_compatible_agg.md)
  - [find_compatible_trans](../f/find_compatible_trans.md)
  - [get_typlenbyval](../g/get_typlenbyval.md)
  - [GetAggInitVal](../G/GetAggInitVal.md)
  - [agg_args_support_sendreceive](../a/agg_args_support_sendreceive.md)
  - [exprTypmod](../e/exprTypmod.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - list_nth_node
  - makeNode (AggInfo, AggTransInfo)
- Called from (representative examples):
  - [preprocess_aggrefs_walker](preprocess_aggrefs_walker.md)

## Notes and Other Information
- This is a static function only called from within the same file
- Modifies the Aggref node in-place by setting aggno, aggtransno, and aggtranstype fields
- Assumes aggref->agglevelsup == 0 (aggregate belongs to current query level)
- Creates and maintains root->agginfos and root->aggtransinfos lists for aggregate optimization
- Handles complex partial aggregation feasibility analysis including serialization requirements
- Special case handling for array_agg serialization functions that depend on element type send/receive functions
- Updates planning flags like numOrderedAggs, hasNonPartialAggs, and hasNonSerialAggs based on aggregate properties