# ExecInitNestLoop

## Location
[src/backend/executor/nodeNestloop.c:262-360](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeNestloop.c#L262-L360)

## Overview
ExecInitNestLoop initializes a NestLoopState node for executing nested loop joins, setting up child plan nodes, expression contexts, and join-specific state information.

## Definition
```c
NestLoopState *ExecInitNestLoop(NestLoop *node, EState *estate, int eflags)
```

## Detailed Description
ExecInitNestLoop is responsible for setting up all the necessary structures and state for executing a nested loop join operation. It creates and initializes the NestLoopState structure, which contains the runtime state needed during join execution.

The function performs several key initialization tasks: creating the state structure, setting up expression contexts, initializing child plan nodes (outer and inner), configuring result tuple slots and projection information, and initializing join and qualification expressions.

A critical aspect of the initialization is handling execution flags for the inner child node. If there are no nest parameters (nestParams), the function enables the EXEC_FLAG_REWIND flag to allow efficient rescanning of the inner relation. However, if nest parameters exist (indicating parameterized nested loops), the REWIND flag is disabled since the inner plan will always be rescanned with fresh parameter values.

The function also sets up null tuple slots for outer joins (LEFT and ANTI joins) to handle cases where no matching inner tuple is found. It determines whether only the first matching inner tuple needs to be considered based on inner relation uniqueness or semi-join semantics.

## Parameters / Member Variables
- `node`: The NestLoop plan node containing join configuration and child plan references
- `estate`: The execution state containing global execution context and memory information  
- `eflags`: Execution flags controlling behavior such as backward scanning and rewind capabilities

## Dependencies
- Functions called/Symbols referenced:
  - makeNode: Creates a new NestLoopState node
  - [ExecAssignExprContext](ExecAssignExprContext.md): Creates expression evaluation context
  - [ExecInitNode](ExecInitNode.md): Recursively initializes outer and inner child plan nodes
  - [ExecInitResultTupleSlotTL](ExecInitResultTupleSlotTL.md): Initializes result tuple slot and type information
  - [ExecAssignProjectionInfo](ExecAssignProjectionInfo.md): Sets up projection infrastructure for result tuples
  - [ExecInitQual](ExecInitQual.md): Compiles qualification and join condition expressions
  - [ExecInitNullTupleSlot](ExecInitNullTupleSlot.md): Creates null tuple slot for outer join handling
  - [ExecGetResultType](ExecGetResultType.md): Gets result tuple descriptor from inner plan
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md): As part of plan tree initialization process

## Notes and Other Information
- Uses NL1_printf debug macros for initialization tracing
- Validates that unsupported flags (EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK) are not set
- Sets ExecProcNode function pointer to ExecNestLoop for tuple production
- Initializes nl_NeedNewOuter to true and nl_MatchedOuter to false for proper join state
- Handles different join types with appropriate null tuple slot setup
- Uses TTSOpsVirtual for virtual tuple slot operations
- Supports both parameterized and non-parameterized nested loop execution

## Simplified Source

```c
NestLoopState *
ExecInitNestLoop(NestLoop *node, EState *estate, int eflags)
{
    NestLoopState *nlstate;

    // Validate execution flags
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    // Create and initialize the nested loop state
    nlstate = makeNode(NestLoopState);
    nlstate->js.ps.plan = (Plan *) node;
    nlstate->js.ps.state = estate;
    nlstate->js.ps.ExecProcNode = ExecNestLoop;

    // Create expression context
    ExecAssignExprContext(estate, &nlstate->js.ps);

    // Initialize child nodes with appropriate rewind flags
    outerPlanState(nlstate) = ExecInitNode(outerPlan(node), estate, eflags);
    if (node->nestParams == NIL)
        eflags |= EXEC_FLAG_REWIND;  // Enable rewind for non-parameterized scans
    else
        eflags &= ~EXEC_FLAG_REWIND; // Disable rewind for parameterized scans
    innerPlanState(nlstate) = ExecInitNode(innerPlan(node), estate, eflags);

    // Initialize result handling
    ExecInitResultTupleSlotTL(&nlstate->js.ps, &TTSOpsVirtual);
    ExecAssignProjectionInfo(&nlstate->js.ps, NULL);

    // Initialize join expressions
    nlstate->js.ps.qual = ExecInitQual(node->join.plan.qual, (PlanState *) nlstate);
    nlstate->js.jointype = node->join.jointype;
    nlstate->js.joinqual = ExecInitQual(node->join.joinqual, (PlanState *) nlstate);

    // Determine if only first match is needed
    nlstate->js.single_match = (node->join.inner_unique || node->join.jointype == JOIN_SEMI);

    // Set up null tuples for outer joins
    switch (node->join.jointype)
    {
        case JOIN_INNER:
        case JOIN_SEMI:
            break;
        case JOIN_LEFT:
        case JOIN_ANTI:
            nlstate->nl_NullInnerTupleSlot =
                ExecInitNullTupleSlot(estate, ExecGetResultType(innerPlanState(nlstate)), &TTSOpsVirtual);
            break;
        default:
            elog(ERROR, "unrecognized join type: %d", (int) node->join.jointype);
    }

    // Initialize join state
    nlstate->nl_NeedNewOuter = true;
    nlstate->nl_MatchedOuter = false;

    return nlstate;
}
```