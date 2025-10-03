# ExecInitHashJoin

## Location
[src/backend/executor/nodeHashjoin.c:710-858](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHashjoin.c#L710-L858)

## Overview
ExecInitHashJoin initializes a HashJoin node during query plan startup, setting up all necessary state structures, child nodes, expression contexts, and tuple slots required for hash join execution.

## Definition

```c
structure
	 */
	hjstate = makeNode(HashJoinState);
```
## Detailed Description
ExecInitHashJoin is the initialization routine for HashJoin nodes in PostgreSQL's executor. It performs comprehensive setup of the hash join execution state, including:

1. **State Structure Creation**: Allocates and initializes a HashJoinState structure
2. **Child Node Initialization**: Recursively initializes outer and inner plan nodes
3. **Expression Context Setup**: Creates expression evaluation contexts for the join
4. **Tuple Slot Management**: Sets up various tuple slots for different join phases
5. **Join-Specific Configuration**: Configures behavior based on join type (inner, left, right, anti, semi, full)
6. **Hash-Specific Initialization**: Sets up hash-related state variables and structures

The function handles special cases for different join types, particularly around null tuple slot creation for outer joins. It also performs an optimization trick where the hash join node reuses the Hash node's result tuple slot as its internal hash tuple slot, since Hash nodes don't return tuples through the normal ExecProcNode() interface.

## Parameters / Member Variables
- : The HashJoin plan node containing join configuration and child plans
- : The execution state containing global execution context
- : Execution flags controlling behavior (EXEC_FLAG_BACKWARD and EXEC_FLAG_MARK are not supported)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode: Creates new HashJoinState structure  
  - [ExecAssignExprContext](ExecAssignExprContext.md): Sets up expression evaluation context
  - [ExecInitNode](ExecInitNode.md): Recursively initializes child plan nodes
  - [ExecGetResultType](ExecGetResultType.md): Retrieves tuple descriptor from child nodes
  - [ExecInitResultTupleSlotTL](ExecInitResultTupleSlotTL.md): Initializes result tuple slot
  - [ExecAssignProjectionInfo](ExecAssignProjectionInfo.md): Sets up tuple projection
  - [ExecInitExtraTupleSlot](ExecInitExtraTupleSlot.md): Creates additional tuple slots
  - [ExecInitNullTupleSlot](ExecInitNullTupleSlot.md): Creates null tuple slots for outer joins
  - [ExecInitQual](ExecInitQual.md): Initializes expression trees for qualifiers
  - [ExecInitExprList](ExecInitExprList.md): Initializes expression lists

- Called from:
  - [ExecInitNode](ExecInitNode.md): General node initialization dispatcher

## Notes and Other Information
Key aspects of the initialization process:

- **Join Type Handling**: The function creates appropriate null tuple slots based on join type - left/anti joins need null inner slots, right/right-anti need null outer slots, and full joins need both
- **Single Match Optimization**: Determines if only the first matching inner tuple needs to be considered (for inner_unique joins or semi-joins)
- **Hash Tuple Slot Trick**: Reuses the Hash node's result tuple slot as the hash tuple slot since Hash nodes don't return tuples via normal execution
- **Expression Initialization**: Sets up all expression trees (join quals, other quals, hash clauses) for runtime evaluation
- **State Reset**: Initializes all hash join state variables to their starting values

The function sets the initial join state to HJ_BUILD_HASHTABLE, indicating that hash table construction is the first operation to perform during execution.

Location: src/backend/executor/nodeHashjoin.c:710-858

## Simplified Source

```c
HashJoinState *
ExecInitHashJoin(HashJoin *node, EState *estate, int eflags)
{
    HashJoinState *hjstate;
    Plan *outerNode;
    Hash *hashNode;
    TupleDesc outerDesc, innerDesc;

    // Verify supported execution flags
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    // Create and initialize main state structure
    hjstate = makeNode(HashJoinState);
    hjstate->js.ps.plan = (Plan *) node;
    hjstate->js.ps.state = estate;
    hjstate->js.ps.ExecProcNode = ExecHashJoin;
    hjstate->js.jointype = node->join.jointype;

    // Set up expression context
    ExecAssignExprContext(estate, &hjstate->js.ps);

    // Initialize child nodes (outer and inner)
    outerNode = outerPlan(node);
    hashNode = (Hash *) innerPlan(node);
    outerPlanState(hjstate) = ExecInitNode(outerNode, estate, eflags);
    innerPlanState(hjstate) = ExecInitNode((Plan *) hashNode, estate, eflags);

    // Get tuple descriptors from child nodes
    outerDesc = ExecGetResultType(outerPlanState(hjstate));
    innerDesc = ExecGetResultType(innerPlanState(hjstate));

    // Initialize result slot and projection
    ExecInitResultTupleSlotTL(&hjstate->js.ps, &TTSOpsVirtual);
    ExecAssignProjectionInfo(&hjstate->js.ps, NULL);

    // Set up outer tuple slot
    hjstate->hj_OuterTupleSlot = ExecInitExtraTupleSlot(estate, outerDesc,
        ExecGetResultSlotOps(outerPlanState(hjstate), NULL));

    // Configure single match optimization
    hjstate->js.single_match = (node->join.inner_unique ||
                                node->join.jointype == JOIN_SEMI);

    // Set up null tuple slots based on join type
    switch (node->join.jointype) {
        case JOIN_INNER:
        case JOIN_SEMI:
            // No null slots needed
            break;
        case JOIN_LEFT:
        case JOIN_ANTI:
            hjstate->hj_NullInnerTupleSlot =
                ExecInitNullTupleSlot(estate, innerDesc, &TTSOpsVirtual);
            break;
        case JOIN_RIGHT:
        case JOIN_RIGHT_ANTI:
            hjstate->hj_NullOuterTupleSlot =
                ExecInitNullTupleSlot(estate, outerDesc, &TTSOpsVirtual);
            break;
        case JOIN_FULL:
            hjstate->hj_NullOuterTupleSlot =
                ExecInitNullTupleSlot(estate, outerDesc, &TTSOpsVirtual);
            hjstate->hj_NullInnerTupleSlot =
                ExecInitNullTupleSlot(estate, innerDesc, &TTSOpsVirtual);
            break;
        default:
            elog(ERROR, "unrecognized join type: %d", (int) node->join.jointype);
    }

    // Reuse Hash node's result slot as hash tuple slot
    HashState *hashstate = (HashState *) innerPlanState(hjstate);
    hjstate->hj_HashTupleSlot = hashstate->ps.ps_ResultTupleSlot;

    // Initialize expression trees
    hjstate->js.ps.qual = ExecInitQual(node->join.plan.qual, (PlanState *) hjstate);
    hjstate->js.joinqual = ExecInitQual(node->join.joinqual, (PlanState *) hjstate);
    hjstate->hashclauses = ExecInitQual(node->hashclauses, (PlanState *) hjstate);

    // Initialize hash-specific state variables
    hjstate->hj_HashTable = NULL;
    hjstate->hj_FirstOuterTupleSlot = NULL;
    hjstate->hj_CurHashValue = 0;
    hjstate->hj_CurBucketNo = 0;
    hjstate->hj_CurSkewBucketNo = INVALID_SKEW_BUCKET_NO;
    hjstate->hj_CurTuple = NULL;

    // Set up hash key expressions and operators
    hjstate->hj_OuterHashKeys = ExecInitExprList(node->hashkeys, (PlanState *) hjstate);
    hjstate->hj_HashOperators = node->hashoperators;
    hjstate->hj_Collations = node->hashcollations;

    // Set initial execution state
    hjstate->hj_JoinState = HJ_BUILD_HASHTABLE;
    hjstate->hj_MatchedOuter = false;
    hjstate->hj_OuterNotEmpty = false;

    return hjstate;
}
```