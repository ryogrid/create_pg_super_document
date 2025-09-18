# ExecBuildUpdateProjection

## Location
src/backend/executor/execExpr.c: 522 - 739

## Overview
Builds a specialized ProjectionInfo node for constructing new tuples during UPDATE operations, handling both pre-computed and dynamically-evaluated SET expressions while preserving unchanged columns from the original tuple.

## Definition


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
  - bms_add_member, bms_is_member (bitmap set operations)
  - expr_setup_walker (expression analysis)
  - ExecPushExprSetupSteps (setup step generation)
  - ExecInitExprRec (expression compilation)
  - ExprEvalPushStep (evaluation step creation)
  - ExecReadyExpr (finalization)
  - format_type_be (error reporting)
- Called from (representative examples):
  - ExecInitUpdateProjection
  - ExecInitPartitionInfo
  - ExecInitMerge
  - ExecInitModifyTable

## Notes and Other Information
- **Specialized variant**: This is a specialized version of ExecBuildProjectionInfo specifically designed for UPDATE operations
- **Safety-first approach**: Incorporates sanity checks that would normally be handled by ExecCheckPlanOutput, since UPDATE projections don't follow the normal target list pattern
- **Performance optimization**: Uses bitmap sets for efficient column membership testing to avoid O(N²) behavior with many columns
- **Tuple slot assumptions**: Relies on specific tuple slot conventions:
  - "scan" slot contains the original tuple being updated
  - "outer" slot contains pre-computed values when evalTargetList is false
- **Dropped column handling**: Explicitly sets dropped columns to NULL in the result tuple to maintain tuple structure consistency
- **Resjunk column handling**: Evaluates resjunk columns when needed but discards their values from the final result