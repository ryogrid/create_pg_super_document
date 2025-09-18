# ProjectionInfo

## Location
[src/include/nodes/execnodes.h:360-367](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L360-L367)

## Overview
ProjectionInfo is a structure used to perform projections in PostgreSQL's executor, which forms new tuples by evaluating targetlist expressions and storing them in result slots.

## Definition


## Detailed Description
ProjectionInfo contains all the information needed to perform projections in PostgreSQL's execution engine. Projection is the process of forming new tuples by evaluating targetlist expressions. Nodes that need to perform projections create one of these structures.

The target tuple slot is kept in . The  function evaluates the target list, forms a tuple, and stores it in the given slot. The result will be a "virtual" tuple unless  is called to convert it to a physical tuple. The slot must have a tuple descriptor that matches the output of the target list.

## Parameters / Member Variables
- : NodeTag identifier for the structure type
- : ExprState containing the instructions to evaluate the projection expressions
- : Expression context in which to evaluate the projection expressions

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag
  - ExprState  
  - ExprContext
- Called from (representative examples):
  - [ExecBuildProjectionInfo](../E/ExecBuildProjectionInfo.md)
  - [ExecScan](../E/ExecScan.md)
  - [ExecProcessReturning](../E/ExecProcessReturning.md)
  - [ExecGetInsertNewTuple](../E/ExecGetInsertNewTuple.md)
  - [ExecGetUpdateNewTuple](../E/ExecGetUpdateNewTuple.md)

## Notes and Other Information
- [ProjectionInfo](ProjectionInfo.md) is fundamental to PostgreSQL's tuple processing pipeline
- The structure enables efficient evaluation of SELECT list expressions
- Used extensively throughout the executor for forming output tuples
- The pi_exprContext provides the runtime environment for expression evaluation
- Virtual tuples created by projection are memory-efficient until materialization is required