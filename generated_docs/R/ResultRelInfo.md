# ResultRelInfo

## Location
[src/include/nodes/execnodes.h:450-596](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L450-L596)

## Overview
ResultRelInfo holds comprehensive information about a result relation, including indexes, triggers, and state needed for INSERT, UPDATE, DELETE, and MERGE operations.

## Definition


## Detailed Description
ResultRelInfo is a comprehensive structure that holds all information needed about a result relation for data modification operations. When updating an existing relation, PostgreSQL must also update indexes and potentially fire triggers. This structure centralizes all the necessary state.

ResultRelInfo can refer to tables in the query's range table (with ri_RangeTableIndex set) or to relations not in the range table, such as partition targets for tuple routing or trigger target tables. The structure supports complex operations including batch inserts, MERGE statements, ON CONFLICT handling, partitioning, and foreign data wrappers.

## Parameters / Member Variables
- : NodeTag identifier for the structure type
- : Result relation's range table index, or 0 if not in range table
- : Relation descriptor for the result relation
- : Number of indices existing on the result relation
- : Array of relation descriptors for indices
- : Array of key/attribute info for indices
- : Attribute number of row identity junk attribute for UPDATE/DELETE
- : Attribute numbers of generated columns to compute for UPDATE
- : ProjectionInfo to generate new tuple in INSERT/UPDATE
- : Slot to hold the new tuple
- : Slot to hold the old tuple being updated
- : Whether projection and slots have been initialized
- : Whether updates need LockTuple() before reading old tuple
- : Triggers to be fired, if any
- : Cached lookup info for trigger functions
- : Array of trigger WHEN expression states
- : Optional runtime measurements for triggers
- , , : On-demand created slots for processing
- : FDW callback functions for foreign tables
- : Private state for FDW
- : Whether modifying foreign table directly
- , : Batch insert configuration and slots
- : List of WithCheckOption constraints
- : Array of constraint-checking expression states
- : Expression states for generated columns
- : List of RETURNING expressions
- : ProjectionInfo for computing RETURNING list
- : List of arbiter indexes for conflict checking
- : ON CONFLICT evaluation state
- : Lists of MergeActionState for MERGE operations
- : Expression state for MERGE join condition
- : Partition check expression state
- : Tuple conversion maps for partitioning
- : Target relation for tuple routing/transition capture
- : Tuple slot for partition routing
- : Buffer for COPY multi-inserts
- : List for cross-partition update handling

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag
  - [Relation](Relation.md)
  - RelationPtr
  - IndexInfo
  - [ProjectionInfo](../P/ProjectionInfo.md)
  - TriggerDesc
  - [OnConflictSetState](../O/OnConflictSetState.md)
  - TupleConversionMap
  - [FdwRoutine](../F/FdwRoutine.md)
  - Various executor data types
- Called from (representative examples):
  - Data modification operations (INSERT, UPDATE, DELETE, MERGE)
  - Trigger execution
  - Partition handling
  - Foreign data wrapper operations

## Notes and Other Information
- Central to PostgreSQL's data modification pipeline
- Supports advanced features like partitioning, UPSERT, MERGE, and foreign tables
- Manages both regular and batch insert operations
- Handles complex constraint checking and trigger execution
- Essential for RETURNING clause processing
- Supports tuple conversion between parent and child partitions
- Integrates with foreign data wrapper architecture
- Critical for maintaining data integrity through constraint and trigger management