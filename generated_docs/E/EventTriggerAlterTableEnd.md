# EventTriggerAlterTableEnd

## Location
src/backend/commands/event_trigger.c: 1713 - 1750

## Overview
Finalizes the collection of an ALTER TABLE command and adds it to the event trigger command list, completing the event trigger data collection process for ALTER TABLE operations.

## Definition


## Detailed Description
This function serves as the completion handler for ALTER TABLE command collection in PostgreSQL's event trigger system. It is called at the end of ALTER TABLE processing to finalize the command collection that was started earlier.

The function performs several key operations:
1. Checks if any subcommands were actually collected during the ALTER TABLE operation
2. If subcommands exist, adds the complete command structure to the event trigger's command list
3. If no subcommands were collected, frees the allocated command structure to avoid memory leaks
4. Restores the parent command as the current command, allowing for nested command processing

The function includes a note about a potential issue with transaction/subtransaction aborts that may need to be addressed with an AtEOSubXact_EventTriggers() function.

## Parameters / Member Variables
This function takes no parameters and operates on the global currentEventTriggerState.

## Dependencies
- Functions called/Symbols referenced:
  -  - Memory context switching
  -  - List append operation
  -  - Memory deallocation
  -  - Empty list constant
- Called from (representative examples):
  -  - Utility command processing
  -  - ALTER TABLE-specific utility processing
  -  - Table movement operations

## Notes and Other Information
- Only operates when event trigger context is active and command collection is not inhibited
- Includes a FIXME comment noting potential issues with transaction abort handling
- Uses the event trigger's memory context to ensure command data persists appropriately
- Part of a paired operation with command start functions that initialize ALTER TABLE collection
- The function properly handles the case where no subcommands were collected, preventing memory leaks
- Supports nested command processing by maintaining a parent command stack