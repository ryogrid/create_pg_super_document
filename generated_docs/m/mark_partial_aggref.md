# mark_partial_aggref

## Location
src/backend/optimizer/plan/planner.c: 5712 - 5746

## Overview
Adjusts an Aggref node in-place to represent a partial-aggregation step as part of PostgreSQL's multi-phase aggregation optimization strategy.

## Definition


## Detailed Description
This function modifies an existing Aggref node to transform it from a simple aggregation into a partial aggregation step. This is a key component of PostgreSQL's parallel aggregation optimization, where aggregation can be split across multiple phases (e.g., partial aggregation in worker processes followed by final aggregation in the leader process).

The function updates both the aggregation split mode and adjusts the result type when necessary. For partial aggregations that skip the final step, the result type is changed from the aggregate's final result type to the transition type, with special handling for INTERNAL types that need serialization.

## Parameters / Member Variables
- : Pointer to the Aggref node to be modified in-place
- : The intended partial-aggregation mode (AggSplit enum value)

## Dependencies
- Functions called/Symbols referenced:
  - AggSplit (enum type)
  - Aggref (struct type)
  - AGGSPLIT_SIMPLE (enum value)
  - DO_AGGSPLIT_SKIPFINAL (macro)
  - DO_AGGSPLIT_SERIALIZE (macro)
  - INTERNALOID (constant)
  - BYTEAOID (constant)
- Called from (representative examples):
  - make_partial_grouping_target (src/backend/optimizer/plan/planner.c:5691)
  - convert_combining_aggrefs (src/backend/optimizer/plan/setrefs.c:2592, 2599)

## Notes and Other Information
- The function assumes that aggtranstype has already been computed and is valid
- The original aggsplit value must be AGGSPLIT_SIMPLE when this function is called
- For partial aggregates that serialize INTERNAL transition values, the result type is changed to BYTEA to enable proper serialization/deserialization across process boundaries
- This function is essential for PostgreSQL's parallel aggregation infrastructure, enabling efficient distribution of aggregation work across multiple processes