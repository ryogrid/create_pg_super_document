# EventTriggerCollectAlterTableSubcmd

## Location
src/backend/commands/event_trigger.c: 1678 - 1712

## Overview
Saves data about a single part of an ALTER TABLE command for event trigger processing, collecting ALTER TABLE subcommands that can be later processed by event triggers.

## Definition


## Detailed Description
This function is responsible for collecting information about individual ALTER TABLE subcommands during command execution. It operates as part of PostgreSQL's event trigger system, which allows users to define triggers that fire in response to DDL commands.

The function creates a  structure to store the subcommand information and adds it to the current event trigger state's list of ALTER TABLE subcommands. This collection process enables event triggers to access detailed information about each part of a complex ALTER TABLE operation.

The function only operates when event trigger context is active and command collection is not inhibited. It ensures that the collected data persists in the appropriate memory context for later use by event triggers.

## Parameters / Member Variables
- : A Node pointer that must be an AlterTableCmd representing the specific ALTER TABLE subcommand being collected
- : An ObjectAddress structure identifying the database object affected by this subcommand

## Dependencies
- Functions called/Symbols referenced:
  -  - Type checking macro
  -  - Assertion macro
  -  - OID validation function  
  -  - Memory context switching
  -  - Memory allocation in current context
  -  - Deep copy of parse tree nodes
  -  - List append operation
- Called from (representative examples):
  -  - ALTER TABLE command execution

## Notes and Other Information
- The function only processes AlterTableCmd nodes, which covers ALTER TABLE operations and some internally generated commands
- Collection is conditional on event trigger state being active and not inhibited
- Uses the event trigger's memory context to ensure collected data persists beyond the current operation
- The collected subcommands are stored in the currentCommand's d.alterTable.subcmds list for later event trigger processing
- Part of PostgreSQL's comprehensive event trigger system that provides hooks into DDL operations