# view_has_instead_trigger

## Location
src/backend/rewrite/rewriteHandler.c: 2511 - 2574

## Overview
Determines whether a view has an INSTEAD OF trigger for a specific command type (INSERT, UPDATE, DELETE, or MERGE), which affects whether the view can be treated as auto-updatable.

## Definition
bool view_has_instead_trigger(Relation view, CmdType event, List *mergeActionList)

## Detailed Description
This function checks if a view has the appropriate INSTEAD OF triggers based on the command type being executed. Views with INSTEAD OF triggers are not considered auto-updatable since the triggers handle the modification logic explicitly. For MERGE operations, the function ensures that all data-modifying actions in the merge have corresponding INSTEAD OF triggers, returning true only if every action (except DO NOTHING) has an appropriate trigger.

The function is crucial in the query rewrite system to determine the correct handling path for view modifications - either through auto-updatable view mechanisms or through trigger execution.

## Parameters / Member Variables
- `view`: The relation representing the view to check for INSTEAD OF triggers
- `event`: The command type (CMD_INSERT, CMD_UPDATE, CMD_DELETE, or CMD_MERGE) to check triggers for  
- `mergeActionList`: List of MergeAction nodes used only when event is CMD_MERGE, specifying the actions that need trigger coverage

## Dependencies
- Functions called/Symbols referenced:
  - CmdType (enum for command types)
  - TriggerDesc (structure containing trigger information)
  - CMD_INSERT, CMD_UPDATE, CMD_DELETE, CMD_MERGE, CMD_NOTHING (command type constants)
  - foreach_node (macro for iterating over list nodes)
  - MergeAction (structure representing merge actions)
- Called from (representative examples):
  - CheckValidResultRel (in src/backend/executor/execMain.c:1055)
  - rewriteValuesRTE (in src/backend/rewrite/rewriteHandler.c:1461)
  - rewriteTargetView (in src/backend/rewrite/rewriteHandler.c:3393)
  - RewriteQuery (in src/backend/rewrite/rewriteHandler.c:4194)

## Notes and Other Information
- The function returns false for views without triggers, allowing them to be considered for auto-updatable view processing
- For MERGE operations, if there are only DO NOTHING actions, the function returns true to treat the view as trigger-updatable rather than generating errors
- This check cannot be integrated into view_query_is_auto_updatable because having INSTEAD OF triggers is not an error condition - it is simply a different execution path
- The function accesses the views trigdesc field to examine trigger flags like trig_insert_instead_row, trig_update_instead_row, and trig_delete_instead_row