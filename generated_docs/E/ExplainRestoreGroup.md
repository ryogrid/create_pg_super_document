# ExplainRestoreGroup

## Location
src/backend/commands/explain.c: 5046 - 5076

## Overview
ExplainRestoreGroup re-establishes the grouping state that was previously saved by ExplainSaveGroup, undoing the effects of the save operation.

## Definition


## Detailed Description
ExplainRestoreGroup completes the deferred output pattern by restoring formatting state that was previously saved with ExplainSaveGroup. This function:

1. Increases the indentation level by the specified depth
2. For JSON and YAML formats, restores the saved grouping information back onto the stack
3. Re-establishes the formatting state to match what was originally set up by ExplainOpenSetAsideGroup

The function handles format-specific state restoration:
- **TEXT format**: No action required
- **XML format**: Simply adjusts indentation back to the expected level
- **JSON/YAML formats**: Restores the saved grouping information to the stack and adjusts indentation

This enables the completion of the deferred output workflow: ExplainOpenSetAsideGroup → content generation → ExplainSaveGroup → main output → ExplainRestoreGroup → buffered content output.

## Parameters / Member Variables
- : ExplainState structure containing formatting information and grouping stack
- : The nesting depth to increase (should match the depth used in ExplainSaveGroup)
- : Pointer to integer storage containing the previously saved grouping state

## Dependencies
- Functions called/Symbols referenced:
  - lcons_int (to restore the saved state back onto the grouping stack)
- Called from (representative examples):
  - ExplainOpenWorker

## Notes and Other Information
- This is a static function, only accessible within the explain.c file
- Must be used in conjunction with ExplainSaveGroup - the depth parameter should match exactly
- Part of the three-step deferred output process: ExplainOpenSetAsideGroup → ExplainSaveGroup → ExplainRestoreGroup
- The function restores state without emitting output - actual content emission happens separately
- Used in worker process output handling where content needs to be formatted and buffered before being merged into the main output
- Located in src/backend/commands/explain.c:5042-5068