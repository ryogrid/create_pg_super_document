# ExecAgg

## Location
[src/backend/executor/nodeAgg.c:2158-2193](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L2158-L2193)

## Overview
ExecAgg is the main execution function for PostgreSQL's aggregate node that receives tuples from its outer subplan and processes aggregates according to the chosen aggregation strategy (plain, sorted, hashed, or mixed).

## Definition


## Detailed Description
ExecAgg serves as the central dispatcher for aggregate processing in PostgreSQL's execution engine. It receives tuples from its outer subplan and aggregates over the appropriate attributes for each aggregate function (Aggref node) appearing in the targetlist or qual of the node. The function supports both grouped and plain aggregation:

- **Grouped aggregation**: Produces a result row for each group
- **Plain aggregation**: Produces a single result row for the entire query

The function dispatches to different processing strategies based on the aggregation strategy:
- **AGG_HASHED**: Uses hash table for grouping, calls agg_fill_hash_table if not already filled, then agg_retrieve_hash_table
- **AGG_MIXED**: Hybrid approach that retrieves from hash table 
- **AGG_PLAIN/AGG_SORTED**: Uses direct retrieval via agg_retrieve_direct

The function maintains state through the AggState structure and continues processing until all aggregates are complete (agg_done flag).

## Parameters / Member Variables
- `pstate`: The PlanState structure cast to AggState containing the aggregate node's execution state

## Dependencies
- Functions called/Symbols referenced:
  - castNode
  - CHECK_FOR_INTERRUPTS
  - [agg_fill_hash_table](../a/agg_fill_hash_table.md)
  - [agg_retrieve_hash_table](../a/agg_retrieve_hash_table.md)
  - [agg_retrieve_direct](../a/agg_retrieve_direct.md)
  - TupIsNull
- Called from (representative examples):
  - [ExecInitAgg](ExecInitAgg.md) (sets this as the execution function)

## Notes and Other Information
- This is a static function within nodeAgg.c, serving as the execution callback for aggregate nodes
- The function uses a switch statement to dispatch based on the aggregation strategy stored in node->phase->aggstrategy
- Returns NULL when aggregation is complete (agg_done is true) or when no more result tuples are available
- The function includes interrupt checking to allow for query cancellation during long-running aggregations
- Each aggregation strategy has its own specialized retrieval function for optimal performance