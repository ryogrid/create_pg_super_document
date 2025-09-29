# ExecBuildProjectionInfo

## Location
[src/backend/executor/execExpr.c:362-521](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExpr.c#L362-L521)

## Overview
Builds a ProjectionInfo node for evaluating a target list in a given expression context and storing results into a tuple slot, implementing efficient projection through compiled expression evaluation.

## Definition

```c
structing a new tuple during UPDATE.
 * The projection will be executed in the given econtext and the result will
 * be stored into the given tuple slot.  (Caller must have ensured that tuple
 * slot has a descriptor matching the target rel!)
 *
 * When evalTargetList is false, targetList contains the UPDATE ... SET
 * expressions that have already been computed by a subplan node;
```
## Detailed Description
ExecBuildProjectionInfo creates a ProjectionInfo node that efficiently evaluates a list of target entries (projection expressions) and stores the results in a specified tuple slot. The function implements an optimized projection mechanism by:

1. **Fast-path optimization**: For simple Var expressions that reference non-system attributes, it uses specialized ASSIGN_*_VAR opcodes (EEOP_ASSIGN_INNER_VAR, EEOP_ASSIGN_OUTER_VAR, EEOP_ASSIGN_SCAN_VAR) to directly copy values without full expression evaluation.

2. **Safety validation**: When inputDesc is provided, it performs compatibility checks between Var expressions and the input tuple descriptor to ensure the relation hasn't changed since plan creation.

3. **Expression compilation**: For complex expressions, it compiles them into an ExprState that can be efficiently executed, with special handling for variable-length data types to ensure read-only access.

4. **Embedded ExprState**: The function embeds an ExprState directly into the ProjectionInfo structure to avoid extra memory allocation.

## Parameters / Member Variables
- : List of TargetEntry nodes representing the expressions to be projected
- : Expression context providing the evaluation environment (variable values, memory context, etc.)
- : TupleTableSlot where the projection results will be stored
- : Parent PlanState node for context and error reporting
- : Optional input tuple descriptor for safety validation of Var expressions (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create ProjectionInfo)
  - [ExecCreateExprSetupSteps](ExecCreateExprSetupSteps.md) (expression setup)
  - [ExecInitExprRec](ExecInitExprRec.md) (expression compilation)
  - [ExprEvalPushStep](ExprEvalPushStep.md) (step compilation)
  - [ExecReadyExpr](ExecReadyExpr.md) (finalize expression)
  - [get_typlen](../g/get_typlen.md) (type length checking)
  - TupleDescAttr (tuple descriptor access)
- Called from (representative examples):
  - [ExecAssignProjectionInfo](ExecAssignProjectionInfo.md)
  - [ExecInitInsertProjection](ExecInitInsertProjection.md)
  - [ExecInitPartitionInfo](ExecInitPartitionInfo.md)
  - [ExecInitSubPlan](ExecInitSubPlan.md)

## Notes and Other Information
- **Version compatibility**: Prior to PostgreSQL v10, targetList was a list of ExprStates; now it should be the planner-created target list since compilation happens in this function
- **Performance optimization**: The fast-path for simple Vars significantly improves performance for common projection scenarios
- **Memory management**: The function embeds ExprState into ProjectionInfo to reduce memory allocation overhead
- **Safety checks**: Input descriptor validation helps catch schema changes that could cause runtime errors
- **Read-only enforcement**: Variable-length projected columns are made read-only to prevent modification issues when referenced multiple times in upper plan nodes

## Simplified Source

```c
ProjectionInfo *
ExecBuildProjectionInfo(List *targetList,
                        ExprContext *econtext,
                        TupleTableSlot *slot,
                        PlanState *parent,
                        TupleDesc inputDesc)
{
    // Create and initialize ProjectionInfo with embedded ExprState
    ProjectionInfo *projInfo = makeNode(ProjectionInfo);
    ExprState *state = &projInfo->pi_state;
    ExprEvalStep step = {0};

    // Set basic properties
    projInfo->pi_exprContext = econtext;
    state->type = T_ExprState;
    state->expr = (Expr *) targetList;
    state->parent = parent;
    state->resultslot = slot;

    // Add any setup steps needed for expression evaluation
    ExecCreateExprSetupSteps(state, (Node *) targetList);

    // Process each target entry in the projection list
    foreach(lc, targetList)
    {
        TargetEntry *tle = lfirst_node(TargetEntry, lc);
        bool use_fast_path = false;

        // Check if this is a simple variable that can use fast path
        if (is_simple_var_reference(tle->expr))
        {
            Var *variable = (Var *) tle->expr;
            AttrNumber attnum = variable->varattno;

            // Validate safety if input descriptor provided
            if (is_safe_var_reference(variable, inputDesc, attnum))
            {
                use_fast_path = true;

                // Choose appropriate fast-path opcode based on variable source
                step.opcode = get_assign_var_opcode(variable->varno);
                step.d.assign_var.attnum = attnum - 1;
                step.d.assign_var.resultnum = tle->resno - 1;
                ExprEvalPushStep(state, &step);
            }
        }

        if (!use_fast_path)
        {
            // Compile complex expression normally
            ExecInitExprRec(tle->expr, state, &state->resvalue, &state->resnull);

            // Choose assignment opcode based on data type characteristics
            if (is_variable_length_type(tle->expr))
                step.opcode = EEOP_ASSIGN_TMP_MAKE_RO;  // Make read-only for safety
            else
                step.opcode = EEOP_ASSIGN_TMP;

            step.d.assign_tmp.resultnum = tle->resno - 1;
            ExprEvalPushStep(state, &step);
        }
    }

    // Add completion step and finalize
    step.opcode = EEOP_DONE;
    ExprEvalPushStep(state, &step);
    ExecReadyExpr(state);

    return projInfo;
}
```