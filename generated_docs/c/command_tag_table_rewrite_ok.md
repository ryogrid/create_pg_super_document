# command_tag_table_rewrite_ok

## Location
src/backend/tcop/cmdtag.c: 72 - 82

## Overview
Returns whether a given command tag allows table rewrite operations, determining if commands associated with the tag can trigger table rewrite event triggers.

## Definition


## Detailed Description
This function provides a boolean indicator of whether a particular PostgreSQL command (identified by its CommandTag) is allowed to perform table rewrite operations. It accesses the  field from the corresponding entry in the global  array. This information is critical for the event trigger system to determine whether table rewrite event triggers should be fired for specific commands.

The function is part of PostgreSQL's command tag infrastructure that categorizes SQL commands and their behavioral properties. Table rewrite operations are significant database events that can trigger specialized event handlers.

## Parameters / Member Variables
- : A CommandTag enum value representing the specific SQL command to check

## Dependencies
- Functions called/Symbols referenced:
  -  (global array of CommandTagBehavior structures)
  - CommandTag (enum type)
- Called from (representative examples):
  -  (src/backend/commands/event_trigger.c:248)
  -  (src/backend/commands/event_trigger.c:673)
  -  (src/include/tcop/cmdtag.h:57)

## Notes and Other Information
- The function performs a simple array lookup using the CommandTag as an index
- Part of the command tag behavior system that includes other properties like event_trigger_ok and display_rowcount
- Used primarily by the event trigger subsystem to determine when table rewrite triggers should be activated
- The underlying data comes from the cmdtaglist.h include file which defines all command behaviors