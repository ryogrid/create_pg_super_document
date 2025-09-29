# ExecBuildUpdateProjection

## Location
[src/backend/executor/execExpr.c:522-739](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExpr.c#L522-L739)

## Overview
Builds a specialized ProjectionInfo node for constructing new tuples during UPDATE operations, handling both pre-computed and dynamically-evaluated SET expressions while preserving unchanged columns from the original tuple.

## Definition

```c
structed.  The scan tuple must be deconstructed at
	 * least as far as the last old column we need.
	 */
	for (int attnum = relDesc->natts;
```
## Detailed Description
ExecBuildUpdateProjection creates a specialized ProjectionInfo for UPDATE operations that efficiently constructs new tuples by:

1. **Dual execution modes**: When evalTargetList is false, it assumes UPDATE SET expressions have been pre-computed and are available in the "outer" tuple slot. When true, it compiles and evaluates the SET expressions directly.

2. **Selective column assignment**: Only columns listed in targetColnos are updated with new values from targetList. All other columns are preserved from the original tuple (assumed to be in the "scan" slot).

3. **Safety validation**: Performs comprehensive sanity checks equivalent to ExecCheckPlanOutput, including:
   - Target list ordering validation (non-junk before junk columns)
   - Column count consistency checks
   - Data type compatibility validation
   - Dropped column detection

4. **Optimal tuple deconstruction**: Calculates the minimum number of attributes that need to be deconstructed from input tuples to avoid unnecessary work.

5. **Complete tuple construction**: Ensures all attributes in the result tuple are properly assigned, including:
   - New values for updated columns
   - Preserved values for unchanged columns  
   - NULL values for dropped columns

## Parameters / Member Variables
- : List of TargetEntry nodes representing UPDATE SET expressions (may include resjunk entries)
- : Boolean indicating whether targetList expressions need evaluation (true) or are pre-computed (false)
- : List of target column numbers corresponding to non-resjunk targetList entries
- : TupleDesc describing the relation being updated (used for validation and column mapping)
- : Expression context for evaluation environment
- : TupleTableSlot for storing the constructed result tuple
- : Parent PlanState node for context

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (ProjectionInfo creation)
  - [bms_add_member](../b/bms_add_member.md), bms_is_member (bitmap set operations)
  - [expr_setup_walker](../e/expr_setup_walker.md) (expression analysis)
  - [ExecPushExprSetupSteps](ExecPushExprSetupSteps.md) (setup step generation)
  - [ExecInitExprRec](ExecInitExprRec.md) (expression compilation)
  - [ExprEvalPushStep](ExprEvalPushStep.md) (evaluation step creation)
  - [ExecReadyExpr](ExecReadyExpr.md) (finalization)
  - [format_type_be](../f/format_type_be.md) (error reporting)
- Called from (representative examples):
  - [ExecInitUpdateProjection](ExecInitUpdateProjection.md)
  - [ExecInitPartitionInfo](ExecInitPartitionInfo.md)
  - [ExecInitMerge](ExecInitMerge.md)
  - [ExecInitModifyTable](ExecInitModifyTable.md)

## Notes and Other Information
- **Specialized variant**: This is a specialized version of ExecBuildProjectionInfo specifically designed for UPDATE operations
- **Safety-first approach**: Incorporates sanity checks that would normally be handled by ExecCheckPlanOutput, since UPDATE projections don't follow the normal target list pattern
- **Performance optimization**: Uses bitmap sets for efficient column membership testing to avoid O(N²) behavior with many columns
- **Tuple slot assumptions**: Relies on specific tuple slot conventions:
  - "scan" slot contains the original tuple being updated
  - "outer" slot contains pre-computed values when evalTargetList is false
- **Dropped column handling**: Explicitly sets dropped columns to NULL in the result tuple to maintain tuple structure consistency
- **Resjunk column handling**: Evaluates resjunk columns when needed but discards their values from the final result

## Simplified Source

```c
ProjectionInfo *
ExecBuildUpdateProjection(List *targetList,
                          bool evalTargetList,
                          List *targetColnos,
                          TupleDesc relDesc,
                          ExprContext *econtext,
                          TupleTableSlot *slot,
                          PlanState *parent)
{
    // Initialize projection state with embedded ExprState
    ProjectionInfo *projInfo = makeNode(ProjectionInfo);
    ExprState *state = &projInfo->pi_state;
    ExprEvalStep step = {0};
    Bitmapset *assignedCols = NULL;
    int nAssignableCols = 0;

    // Set basic properties
    projInfo->pi_exprContext = econtext;
    state->type = T_ExprState;
    state->expr = evalTargetList ? (Expr *) targetList : NULL;
    state->parent = parent;
    state->resultslot = slot;

    // Validate target list structure and count assignable columns
    validate_target_list_structure(targetList, targetColnos, &nAssignableCols);

    // Build bitmap of assigned column numbers for efficient lookup
    assignedCols = build_assigned_columns_bitmap(targetColnos);

    // Determine minimum tuple deconstruction requirements
    ExprSetupInfo deform = calculate_deform_requirements(relDesc, assignedCols,
                                                        targetList, evalTargetList,
                                                        nAssignableCols);

    // Add setup steps for tuple deconstruction
    ExecPushExprSetupSteps(state, &deform);

    // Process assignable columns (non-junk entries)
    int outerattnum = 0;
    forboth(lc, targetList, lc2, targetColnos)
    {
        TargetEntry *tle = lfirst_node(TargetEntry, lc);
        AttrNumber targetattnum = lfirst_int(lc2);

        if (tle->resjunk)
            break;  // Only process non-junk columns

        // Perform safety validation
        validate_target_column(tle, targetattnum, relDesc);

        // Generate assignment code
        if (evalTargetList)
        {
            // Compile and evaluate the SET expression
            ExecInitExprRec(tle->expr, state, &state->resvalue, &state->resnull);
            add_assignment_step(state, EEOP_ASSIGN_TMP, targetattnum - 1);
        }
        else
        {
            // Direct assignment from outer tuple
            add_var_assignment_step(state, EEOP_ASSIGN_OUTER_VAR,
                                  outerattnum, targetattnum - 1);
        }
        outerattnum++;
    }

    // Handle unchanged columns and dropped columns
    for (int attnum = 1; attnum <= relDesc->natts; attnum++)
    {
        Form_pg_attribute attr = TupleDescAttr(relDesc, attnum - 1);

        if (attr->attisdropped)
        {
            // Set dropped columns to NULL
            add_null_assignment(state, attnum - 1);
        }
        else if (!bms_is_member(attnum, assignedCols))
        {
            // Copy unchanged columns from original tuple
            add_var_assignment_step(state, EEOP_ASSIGN_SCAN_VAR,
                                  attnum - 1, attnum - 1);
        }
    }

    // Finalize and return
    step.opcode = EEOP_DONE;
    ExprEvalPushStep(state, &step);
    ExecReadyExpr(state);

    return projInfo;
}
```