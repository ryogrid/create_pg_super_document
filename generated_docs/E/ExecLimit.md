# ExecLimit

## Location
[src/backend/executor/nodeLimit.c:40-352](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeLimit.c#L40-L352)

## Overview
ExecLimit implements the execution logic for LIMIT/OFFSET filtering, managing a state machine that controls tuple flow from a subplan to enforce row count limits and offset requirements.

## Definition

```c
static TupleTableSlot *			/* return: a tuple or NULL */
ExecLimit(PlanState *pstate)
```
## Detailed Description
ExecLimit is the main execution function for PostgreSQL's LIMIT node, implementing a sophisticated state machine to handle various LIMIT/OFFSET scenarios including support for WITH TIES semantics. The function processes tuples from its subplan and applies filtering based on computed offset and count values.

The state machine handles multiple execution states:
- **LIMIT_INITIAL**: First call, computes limit/offset parameters
- **LIMIT_RESCAN**: Resets to start of result window
- **LIMIT_INWINDOW**: Normal processing within the limit window
- **LIMIT_EMPTY**: No tuples to return (empty window or subplan exhausted)
- **LIMIT_WINDOWEND**: Reached end of limit window
- **LIMIT_WINDOWEND_TIES**: Processing ties at window boundary (WITH TIES)
- **LIMIT_SUBPLANEOF**: Subplan reached EOF
- **LIMIT_WINDOWSTART**: Backing off from window start

The function supports both forward and backward scanning directions and handles the complex logic for WITH TIES, which requires comparing tuples to determine if they have equivalent ORDER BY values.

## Parameters / Member Variables
- `*pstate`: Plan state containing the LimitState node and execution context
## Dependencies
- Functions called/Symbols referenced:
  - [recompute_limits](../r/recompute_limits.md) (computes offset/count on first call)
  - [ExecProcNode](ExecProcNode.md) (fetches tuples from subplan)
  - [ExecCopySlot](ExecCopySlot.md) (saves tuple for WITH TIES comparison)  
  - [ExecQualAndReset](ExecQualAndReset.md) (compares tuples for WITH TIES logic)
  - ScanDirectionIsForward (checks scan direction)
  - TupIsNull (checks for null tuples)
  - outerPlanState (accesses subplan state)
- Called from (representative examples):
  - [ExecInitLimit](ExecInitLimit.md) (sets as execution function)

## Notes and Other Information
- Uses a complex state machine to handle different execution phases and edge cases
- Supports WITH TIES semantics by saving the last in-window tuple for comparison
- Handles both forward and backward scan directions with appropriate state transitions
- Position tracking is maintained across state transitions for proper offset/limit enforcement
- Error handling includes checks for subplan failures during backward scanning
- The function is designed to work with rescans while maintaining parallel execution compatibility

## Simplified Source

```c
// Simplified version of ExecLimit
static TupleTableSlot *
ExecLimit(PlanState *pstate)
{
    LimitState *node = castNode(LimitState, pstate);
    ExprContext *econtext = node->ps.ps_ExprContext;
    ScanDirection direction = node->ps.state->es_direction;
    TupleTableSlot *slot;
    PlanState *outerPlan = outerPlanState(node);

    // State machine for LIMIT/OFFSET processing
    switch (node->lstate)
    {
        case LIMIT_INITIAL:
            // First call - compute limit/offset values
            recompute_limits(node);
            // Fall through to LIMIT_RESCAN

        case LIMIT_RESCAN:
            // Don't support backward scans from start
            if (!ScanDirectionIsForward(direction))
                return NULL;

            // Check for empty window
            if (node->count <= 0 && !node->noCount)
            {
                node->lstate = LIMIT_EMPTY;
                return NULL;
            }

            // Skip OFFSET tuples
            for (;;)
            {
                slot = ExecProcNode(outerPlan);
                if (TupIsNull(slot))
                {
                    node->lstate = LIMIT_EMPTY;
                    return NULL;
                }

                // Save last tuple for WITH TIES comparison
                if (node->limitOption == LIMIT_OPTION_WITH_TIES &&
                    node->position - node->offset == node->count - 1)
                {
                    ExecCopySlot(node->last_slot, slot);
                }

                node->subSlot = slot;
                if (++node->position > node->offset)
                    break;  // Found first tuple to return
            }

            node->lstate = LIMIT_INWINDOW;
            break;

        case LIMIT_EMPTY:
            return NULL;

        case LIMIT_INWINDOW:
            if (ScanDirectionIsForward(direction))
            {
                // Check if we've reached the limit
                if (!node->noCount &&
                    node->position - node->offset >= node->count)
                {
                    if (node->limitOption == LIMIT_OPTION_COUNT)
                    {
                        node->lstate = LIMIT_WINDOWEND;
                        return NULL;
                    }
                    else
                    {
                        node->lstate = LIMIT_WINDOWEND_TIES;
                        // Fall through to handle ties
                    }
                }
                else
                {
                    // Get next tuple
                    slot = ExecProcNode(outerPlan);
                    if (TupIsNull(slot))
                    {
                        node->lstate = LIMIT_SUBPLANEOF;
                        return NULL;
                    }

                    // Save for WITH TIES if this will be the last
                    if (node->limitOption == LIMIT_OPTION_WITH_TIES &&
                        node->position - node->offset == node->count - 1)
                    {
                        ExecCopySlot(node->last_slot, slot);
                    }

                    node->subSlot = slot;
                    node->position++;
                    break;
                }
            }
            else
            {
                // Backward scan handling
                if (node->position <= node->offset + 1)
                {
                    node->lstate = LIMIT_WINDOWSTART;
                    return NULL;
                }

                slot = ExecProcNode(outerPlan);
                if (TupIsNull(slot))
                    elog(ERROR, "LIMIT subplan failed to run backwards");

                node->subSlot = slot;
                node->position--;
                break;
            }

            if (node->lstate != LIMIT_WINDOWEND_TIES)
                break;
            // Fall through for WITH TIES processing

        case LIMIT_WINDOWEND_TIES:
            if (ScanDirectionIsForward(direction))
            {
                // Get next tuple and check if it ties with last
                slot = ExecProcNode(outerPlan);
                if (TupIsNull(slot))
                {
                    node->lstate = LIMIT_SUBPLANEOF;
                    return NULL;
                }

                // Compare with saved last tuple
                econtext->ecxt_innertuple = slot;
                econtext->ecxt_outertuple = node->last_slot;
                if (ExecQualAndReset(node->eqfunction, econtext))
                {
                    // Tuple ties - include it
                    node->subSlot = slot;
                    node->position++;
                }
                else
                {
                    // No tie - we're done
                    node->lstate = LIMIT_WINDOWEND;
                    return NULL;
                }
            }
            else
            {
                // Backward scan in TIES mode
                if (node->position <= node->offset + 1)
                {
                    node->lstate = LIMIT_WINDOWSTART;
                    return NULL;
                }

                slot = ExecProcNode(outerPlan);
                if (TupIsNull(slot))
                    elog(ERROR, "LIMIT subplan failed to run backwards");

                node->subSlot = slot;
                node->position--;
                node->lstate = LIMIT_INWINDOW;
            }
            break;

        case LIMIT_SUBPLANEOF:
        case LIMIT_WINDOWEND:
        case LIMIT_WINDOWSTART:
            // Handle backward scan recovery cases
            if (ScanDirectionIsForward(direction))
            {
                if (node->lstate == LIMIT_WINDOWSTART)
                {
                    slot = node->subSlot;
                    node->lstate = LIMIT_INWINDOW;
                    break;
                }
                return NULL;
            }
            else
            {
                // Backward scan recovery
                slot = ExecProcNode(outerPlan);
                if (TupIsNull(slot))
                    elog(ERROR, "LIMIT subplan failed to run backwards");

                node->subSlot = slot;
                node->lstate = LIMIT_INWINDOW;
                break;
            }

        default:
            elog(ERROR, "impossible LIMIT state: %d", (int) node->lstate);
            slot = NULL;
            break;
    }

    return slot;
}
```