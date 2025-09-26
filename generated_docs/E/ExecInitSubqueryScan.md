# ExecInitSubqueryScan

## Location
[src/backend/executor/nodeSubqueryscan.c:97-167](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSubqueryscan.c#L97-L167)

## Overview
ExecInitSubqueryScan initializes a SubqueryScan node for execution, setting up the necessary state structures and initializing the underlying subplan.

## Definition
```c
SubqueryScanState *ExecInitSubqueryScan(SubqueryScan *node, EState *estate, int eflags)
```

## Detailed Description
ExecInitSubqueryScan is responsible for the complete initialization of a subquery scan execution node. It creates and configures the SubqueryScanState structure, initializes the underlying subplan through recursive calls to ExecInitNode, and sets up scan slots and projection information. The function follows PostgreSQL's standard executor initialization pattern, ensuring proper setup of expression contexts, result types, and slot operations. A key aspect of this function is its handling of slot operations, where it optimizes by reusing the subplan's result slot operations rather than creating separate ones, since subquery scans typically don't transform the underlying tuples.

## Parameters / Member Variables
- `node`: A SubqueryScan pointer containing the plan node information and subplan to be executed
- `estate`: An EState pointer providing the execution state context for the query
- `eflags`: Integer flags controlling execution behavior (must not include EXEC_FLAG_MARK for subquery scans)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates new SubqueryScanState structure)
  - [ExecSubqueryScan](ExecSubqueryScan.md) (sets as the execution function)
  - [ExecAssignExprContext](ExecAssignExprContext.md) (creates expression evaluation context)
  - [ExecInitNode](ExecInitNode.md) (recursively initializes the subplan)
  - [ExecInitScanTupleSlot](ExecInitScanTupleSlot.md) (initializes scan tuple slot)
  - [ExecGetResultType](ExecGetResultType.md) (gets result tuple descriptor from subplan)
  - [ExecGetResultSlotOps](ExecGetResultSlotOps.md) (gets slot operations from subplan)
  - [ExecInitResultTypeTL](ExecInitResultTypeTL.md) (initializes result type from target list)
  - [ExecAssignScanProjectionInfo](ExecAssignScanProjectionInfo.md) (sets up projection information)
  - [ExecInitQual](ExecInitQual.md) (initializes qualification expressions)
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md) (part of the general executor node initialization framework)

## Notes and Other Information
- Returns a fully initialized SubqueryScanState structure ready for execution
- Includes assertions to verify that EXEC_FLAG_MARK is not set and that the node has no outer or inner plans
- Optimization: Reuses subplan's slot operations to avoid unnecessary slot operation overhead
- Sets both scan and result operations to the same values since subquery scans don't transform tuples
- Part of PostgreSQL's executor initialization framework that ensures consistent setup across all node types
- Located at src/backend/executor/nodeSubqueryscan.c:97-167