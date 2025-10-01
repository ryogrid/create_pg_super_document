# ExecInitMergeJoin

## Location
[src/backend/executor/nodeMergejoin.c:1444-1640](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMergejoin.c#L1444-L1640)

## Overview
Initializes a MergeJoinState node by setting up all necessary data structures, expression contexts, child nodes, and join-specific configuration for merge join execution.

## Definition
```c
MergeJoinState *ExecInitMergeJoin(MergeJoin *node, EState *estate, int eflags)
```

## Detailed Description
ExecInitMergeJoin performs comprehensive initialization of a merge join node, transforming a plan node into an executable state structure. This function is responsible for setting up all the complex machinery required for merge join execution, including multiple expression contexts, child node initialization, null tuple handling for outer joins, and merge clause preprocessing.

The initialization process involves several critical steps:
- **State structure creation**: Allocates and initializes the MergeJoinState structure with proper linkage to the plan and estate
- **Expression context management**: Creates three expression contexts - the main context and two additional contexts for evaluating join expressions from left and right input tuples
- **Child node initialization**: Recursively initializes both outer and inner child plans, with special handling for mark/restore capabilities
- **Join type configuration**: Sets up appropriate flags and null tuple slots based on the specific join type (INNER, LEFT, RIGHT, FULL, SEMI, ANTI)
- **Merge clause preprocessing**: Analyzes and prepares merge clauses with operator families, collations, and comparison strategies
- **Optimization detection**: Determines whether mark/restore operations can be skipped and whether extra marks are beneficial

The function also performs important validation, ensuring that right joins and full joins only use merge-joinable conditions and that unsupported execution flags are not specified.

## Parameters / Member Variables
- `node`: Pointer to the MergeJoin plan node containing the join configuration, merge clauses, and child plan references
- `estate`: Pointer to the execution state containing global execution context, memory management, and parameter information
- `eflags`: Execution flags controlling behavior; EXEC_FLAG_BACKWARD and EXEC_FLAG_MARK are explicitly not supported

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (memory allocation)
  - [ExecAssignExprContext](ExecAssignExprContext.md) (expression context setup)
  - [CreateExprContext](../C/CreateExprContext.md) (additional context creation)
  - [ExecInitNode](ExecInitNode.md) (child node initialization)
  - [ExecGetResultType](ExecGetResultType.md)/ExecGetResultSlotOps (type information)
  - [ExecInitResultTupleSlotTL](ExecInitResultTupleSlotTL.md) (result slot setup)
  - [ExecAssignProjectionInfo](ExecAssignProjectionInfo.md) (projection configuration)
  - [ExecInitExtraTupleSlot](ExecInitExtraTupleSlot.md) (marked tuple slot)
  - [ExecInitNullTupleSlot](ExecInitNullTupleSlot.md) (outer join null handling)
  - [ExecInitQual](ExecInitQual.md) (qualification expression setup)
  - [MJExamineQuals](../M/MJExamineQuals.md) (merge clause preprocessing)
  - [check_constant_qual](../c/check_constant_qual.md) (join qualification validation)
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md) (executor node initialization dispatcher)

## Notes and Other Information
- Returns a fully initialized MergeJoinState ready for execution
- Validates that right and full joins use only merge-joinable conditions
- Sets up mark/restore optimization based on inner plan type and execution flags
- Creates null tuple slots only for join types that require them (outer joins)
- The mj_ExtraMarks optimization is enabled only for Material nodes without REWIND flag
- [IndexScan](../I/IndexScan.md) and IndexOnlyScan explicitly cannot use extra marks due to positioning limitations
- Merge clauses are preprocessed to extract comparison operators, strategies, and sort information
- Initial join state is always set to EXEC_MJ_INITIALIZE_OUTER to begin execution
- Supports all PostgreSQL join types with appropriate semantic configuration

## Simplified Source

```c
MergeJoinState *
ExecInitMergeJoin(MergeJoin *node, EState *estate, int eflags)
{
    MergeJoinState *mergestate;
    TupleDesc outerDesc, innerDesc;
    const TupleTableSlotOps *innerOps;

    // Validate execution flags - backward scan and mark/restore not supported
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    // Create and initialize state structure
    mergestate = makeNode(MergeJoinState);
    mergestate->js.ps.plan = (Plan *) node;
    mergestate->js.ps.state = estate;
    mergestate->js.ps.ExecProcNode = ExecMergeJoin;
    mergestate->js.jointype = node->join.jointype;
    mergestate->mj_ConstFalseJoin = false;

    // Set up expression contexts
    ExecAssignExprContext(estate, &mergestate->js.ps);
    mergestate->mj_OuterEContext = CreateExprContext(estate);
    mergestate->mj_InnerEContext = CreateExprContext(estate);

    // Configure mark/restore optimization
    Assert(node->join.joinqual == NIL || !node->skip_mark_restore);
    mergestate->mj_SkipMarkRestore = node->skip_mark_restore;

    // Initialize child nodes
    outerPlanState(mergestate) = ExecInitNode(outerPlan(node), estate, eflags);
    outerDesc = ExecGetResultType(outerPlanState(mergestate));

    innerPlanState(mergestate) = ExecInitNode(innerPlan(node), estate,
                                             mergestate->mj_SkipMarkRestore ?
                                             eflags : (eflags | EXEC_FLAG_MARK));
    innerDesc = ExecGetResultType(innerPlanState(mergestate));

    // Set up extra marks optimization for Material nodes
    if (IsA(innerPlan(node), Material) &&
        (eflags & EXEC_FLAG_REWIND) == 0 &&
        !mergestate->mj_SkipMarkRestore)
        mergestate->mj_ExtraMarks = true;
    else
        mergestate->mj_ExtraMarks = false;

    // Initialize result slot and projection
    ExecInitResultTupleSlotTL(&mergestate->js.ps, &TTSOpsVirtual);
    ExecAssignProjectionInfo(&mergestate->js.ps, NULL);

    // Set up marked tuple slot for inner relation
    innerOps = ExecGetResultSlotOps(innerPlanState(mergestate), NULL);
    mergestate->mj_MarkedTupleSlot = ExecInitExtraTupleSlot(estate, innerDesc, innerOps);

    // Initialize qualification expressions
    mergestate->js.ps.qual = ExecInitQual(node->join.plan.qual, (PlanState *) mergestate);
    mergestate->js.joinqual = ExecInitQual(node->join.joinqual, (PlanState *) mergestate);

    // Configure single match optimization
    mergestate->js.single_match = (node->join.inner_unique ||
                                  node->join.jointype == JOIN_SEMI);

    // Set up null tuples based on join type
    switch (node->join.jointype)
    {
        case JOIN_INNER:
        case JOIN_SEMI:
            mergestate->mj_FillOuter = false;
            mergestate->mj_FillInner = false;
            break;
        case JOIN_LEFT:
        case JOIN_ANTI:
            mergestate->mj_FillOuter = true;
            mergestate->mj_FillInner = false;
            mergestate->mj_NullInnerTupleSlot =
                ExecInitNullTupleSlot(estate, innerDesc, &TTSOpsVirtual);
            break;
        case JOIN_RIGHT:
        case JOIN_RIGHT_ANTI:
            mergestate->mj_FillOuter = false;
            mergestate->mj_FillInner = true;
            mergestate->mj_NullOuterTupleSlot =
                ExecInitNullTupleSlot(estate, outerDesc, &TTSOpsVirtual);

            // Validate that right joins use only merge-joinable conditions
            if (!check_constant_qual(node->join.joinqual, &mergestate->mj_ConstFalseJoin))
                ereport(ERROR,
                       (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("RIGHT JOIN is only supported with merge-joinable join conditions")));
            break;
        case JOIN_FULL:
            mergestate->mj_FillOuter = true;
            mergestate->mj_FillInner = true;
            mergestate->mj_NullOuterTupleSlot =
                ExecInitNullTupleSlot(estate, outerDesc, &TTSOpsVirtual);
            mergestate->mj_NullInnerTupleSlot =
                ExecInitNullTupleSlot(estate, innerDesc, &TTSOpsVirtual);

            // Validate that full joins use only merge-joinable conditions
            if (!check_constant_qual(node->join.joinqual, &mergestate->mj_ConstFalseJoin))
                ereport(ERROR,
                       (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("FULL JOIN is only supported with merge-joinable join conditions")));
            break;
        default:
            elog(ERROR, "unrecognized join type: %d", (int) node->join.jointype);
    }

    // Preprocess merge clauses
    mergestate->mj_NumClauses = list_length(node->mergeclauses);
    mergestate->mj_Clauses = MJExamineQuals(node->mergeclauses,
                                           node->mergeFamilies,
                                           node->mergeCollations,
                                           node->mergeStrategies,
                                           node->mergeNullsFirst,
                                           (PlanState *) mergestate);

    // Initialize join execution state
    mergestate->mj_JoinState = EXEC_MJ_INITIALIZE_OUTER;
    mergestate->mj_MatchedOuter = false;
    mergestate->mj_MatchedInner = false;
    mergestate->mj_OuterTupleSlot = NULL;
    mergestate->mj_InnerTupleSlot = NULL;

    return mergestate;
}
```