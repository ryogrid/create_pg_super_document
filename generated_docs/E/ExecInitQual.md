# ExecInitQual

## Location
[src/backend/executor/execExpr.c:221-306](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExpr.c#L221-L306)

## Overview
ExecInitQual prepares a conjunctive boolean expression (qualification list with implicit AND semantics) for execution by ExecQual, optimized for WHERE clause evaluation.

## Definition

```c
ExprState *
ExecInitQual(List *qual, PlanState *parent)
```
## Detailed Description
ExecInitQual is specialized for preparing qualification expressions that represent conjunctive boolean conditions (multiple expressions connected by implicit AND). It implements SQL's three-valued logic where NULL results in a qualification are treated as FALSE, making it particularly suitable for WHERE clause evaluation.

The function implements several key optimizations:
1. **Empty list optimization**: Returns NULL for empty qualification lists (representing TRUE), which hot-spot callers can detect to avoid ExecQual calls entirely
2. **Short-circuit evaluation**: Uses the EEOP_QUAL opcode to immediately return FALSE when any subexpression evaluates to NULL or FALSE
3. **Jump target management**: Builds a series of evaluation steps with proper jump targets to skip remaining expressions when a FALSE/NULL is found

The compilation process creates evaluation steps for each qualification expression, followed by EEOP_QUAL steps that check for FALSE/NULL results and jump to the end if found. The final result is TRUE only if all subexpressions evaluate to TRUE.

## Parameters / Member Variables
- `*qual`: A List of expression nodes representing the conjunctive qualification. Returns NULL if the list is empty (NIL) for optimization.
- `*parent`: The PlanState node that owns this qualification expression.
## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates new ExprState)
  - [ExecCreateExprSetupSteps](ExecCreateExprSetupSteps.md) (inserts setup steps)
  - [ExecInitExprRec](ExecInitExprRec.md) (recursively compiles each qualification expression)
  - [ExprEvalPushStep](ExprEvalPushStep.md) (adds evaluation steps)
  - [ExecReadyExpr](ExecReadyExpr.md) (finalizes expression for execution)
  - [lappend_int](../l/lappend_int.md) (builds jump target adjustment list)
  - foreach_ptr, foreach_int (list iteration macros)
  - EEOP_QUAL, EEOP_DONE (opcode constants)
  - EEO_FLAG_IS_QUAL (marks expression for qualification use)
- Called from (representative examples):
  - [ExecInitSeqScan](ExecInitSeqScan.md) (for table scan qualifications)
  - [ExecInitIndexScan](ExecInitIndexScan.md) (for index scan qualifications) 
  - [ExecInitNestLoop](ExecInitNestLoop.md) (for join qualifications)
  - [ExecInitHashJoin](ExecInitHashJoin.md) (for hash join qualifications)
  - [ExecInitModifyTable](ExecInitModifyTable.md) (for modification qualifications)

## Notes and Other Information
- Implements SQL's three-valued logic: NULL qualification results are treated as FALSE
- Uses EEO_FLAG_IS_QUAL flag to mark the ExprState for qualification-specific evaluation
- Heavily optimized for performance since qualification evaluation is a critical hot path
- The EEOP_QUAL opcode provides simpler and faster evaluation than the general BOOL_AND opcode
- Jump targets are adjusted after compilation to point to the correct end position
- Widely used throughout the executor for filtering tuples based on WHERE conditions
- The resulting ExprState can only be used with ExecQual, not general ExecEvalExpr

## Simplified Source

```c
ExprState *
ExecInitQual(List *qual, PlanState *parent)
{
    // Short-circuit for empty qualification list - always TRUE
    if (qual == NIL)
        return NULL;

    // Initialize ExprState for qualification evaluation
    ExprState *state = makeNode(ExprState);
    ExprEvalStep step = {0};
    List *jump_fixups = NIL;

    // Set basic properties
    state->expr = (Expr *) qual;
    state->parent = parent;
    state->ext_params = NULL;
    state->flags = EEO_FLAG_IS_QUAL;  // Mark as qualification expression

    // Add setup steps for tuple access, parameters, etc.
    ExecCreateExprSetupSteps(state, (Node *) qual);

    // Prepare QUAL step template for short-circuit evaluation
    step.opcode = EEOP_QUAL;
    step.resvalue = &state->resvalue;
    step.resnull = &state->resnull;

    // Compile each qualification expression with short-circuit logic
    foreach_ptr(Expr, expr_node, qual)
    {
        // Compile the individual qualification expression
        ExecInitExprRec(expr_node, state, &state->resvalue, &state->resnull);

        // Add QUAL step that jumps to end if FALSE or NULL
        step.d.qualexpr.jumpdone = -1;  // Will be fixed up later
        ExprEvalPushStep(state, &step);

        // Remember this step for jump target fixup
        jump_fixups = lappend_int(jump_fixups, state->steps_len - 1);
    }

    // Fix up all jump targets to point to the end
    foreach_int(jump_pos, jump_fixups)
    {
        ExprEvalStep *qual_step = &state->steps[jump_pos];
        qual_step->d.qualexpr.jumpdone = state->steps_len;
    }

    // Add final DONE step - if we reach here, all quals were TRUE
    step.opcode = EEOP_DONE;
    ExprEvalPushStep(state, &step);

    ExecReadyExpr(state);
    return state;
}
```