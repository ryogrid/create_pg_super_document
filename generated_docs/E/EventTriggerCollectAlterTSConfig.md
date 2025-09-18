# EventTriggerCollectAlterTSConfig

## Location
src/backend/commands/event_trigger.c: 1862 - 1896

## Overview
Collects metadata about an ALTER TEXT SEARCH CONFIGURATION command being executed for event trigger processing.

## Definition


## Detailed Description
This function is part of PostgreSQL's event trigger system and is responsible for capturing information about ALTER TEXT SEARCH CONFIGURATION commands. When an ALTER TEXT SEARCH CONFIGURATION command is executed, this function creates a CollectedCommand structure containing the command details and stores it in the current event trigger state for later processing by event triggers.

The function operates within the event trigger collection framework and respects the current event trigger context. If event triggers are disabled or collection is inhibited, the function returns early without collecting any information.

## Parameters / Member Variables
- `stmt`: Pointer to the AlterTSConfigurationStmt parse tree structure representing the ALTER command
- `cfgId`: OID of the text search configuration being altered
- `dictIds`: Array of OIDs representing the dictionaries involved in the configuration change
- `ndicts`: Number of dictionaries in the dictIds array

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextSwitchTo
  - palloc0
  - ObjectAddressSet
  - palloc
  - memcpy
  - copyObject
  - lappend
- Called from (representative examples):
  - MakeConfigurationMapping (src/backend/commands/tsearchcmds.c:1484)
  - DropConfigurationMapping (src/backend/commands/tsearchcmds.c:1552)

## Notes and Other Information
- Part of the event trigger collection system that tracks DDL commands for event trigger processing
- Uses SCT_AlterTSConfig command type for classification
- Memory allocation is performed in the event trigger context to ensure proper lifetime management
- The function creates a deep copy of the parse tree using copyObject to ensure data persistence
- Early return behavior when event triggers are disabled or collection is inhibited prevents unnecessary overhead