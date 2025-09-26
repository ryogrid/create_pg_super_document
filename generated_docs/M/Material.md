# Material

## Location
src/include/nodes/plannodes.h: 880 - 883

## Overview
Material is a plan node that materializes the output of its subplan into a tuplestore, enabling multiple passes over the data and supporting backward scanning operations.

## Definition


## Detailed Description
The Material node is one of the simplest plan nodes in PostgreSQL, serving as a buffer that materializes (stores) the entire output of its child plan into a tuplestore. Despite its simple structure containing only the base Plan fields, it serves several critical functions in query execution.

The Material node enables operations that require multiple passes over data, such as merge joins where the inner relation needs to be scanned multiple times, or operations requiring backward scanning. It acts as a caching layer that stores tuples in memory (or spills to disk if necessary) so they can be retrieved multiple times without re-executing the underlying subplan.

When executed, the Material node initially passes through tuples from its subplan while simultaneously storing them in a tuplestore. Once all data has been materialized, subsequent access patterns (forward, backward, rescan, mark/restore) can be satisfied directly from the tuplestore without re-executing the expensive subplan.

This node is automatically inserted by the planner when it determines that a subplan will need to be accessed multiple times or when backward scanning capabilities are required by parent nodes.

## Parameters / Member Variables
- : Base Plan structure containing target list, qualifications, cost estimates, and child plan references

## Dependencies
- Functions called/Symbols referenced:
  - Plan (inherited base structure)

- Called from (representative examples):
  - ExecInitNode (executor/execProcnode.c:316)
  - ExecInitMaterial (executor/nodeMaterial.c:164)
  - ExecInitMergeJoin (executor/nodeMergejoin.c:1514)
  - create_material_plan (optimizer/plan/createplan.c:1641)
  - make_material (optimizer/plan/createplan.c:6508)

## Notes and Other Information
- Material nodes are often inserted automatically by the query planner rather than being explicitly requested
- The node uses PostgreSQL's tuplestore facility to cache data, which can spill to disk for large datasets
- Commonly used as input to merge joins where the inner relation needs to support mark/restore operations
- Essential for operations requiring backward scanning or multiple iterations over the same dataset
- The actual materialization is lazy - tuples are stored as they are first requested, not all at once
- Memory usage is controlled by work_mem setting, with automatic spilling to temporary files when necessary
- Despite its simple structure, the runtime state (MaterialState) maintains complex tuplestore management logic