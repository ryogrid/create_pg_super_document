# EventTriggerAlterTableRelid

## Location
src/backend/commands/event_trigger.c: 1660 - 1677

## Overview
Records the OID of the relation being affected by an ALTER TABLE command in the current event trigger command structure.

## Definition
```c
void EventTriggerAlterTableRelid(Oid objectId)
```

## Detailed Description
This function complements EventTriggerAlterTableStart by setting the actual relation OID in the ALTER TABLE command that is currently being collected. Since ALTER TABLE commands can be complex and the target relation OID may not be immediately available when EventTriggerAlterTableStart is called, this function provides a way to update the objectId field once the OID is determined.

The function directly modifies the currentCommand's objectId field, which was initially set to InvalidOid by EventTriggerAlterTableStart.

## Parameters / Member Variables
- `objectId`: The OID of the relation being altered by the current ALTER TABLE command

## Dependencies
- Functions called/Symbols referenced:
  - currentEventTriggerState (global state variable)
  - currentCommand (nested field access)
- Called from (representative examples):
  - [AlterTableInternal](../A/AlterTableInternal.md) (src/backend/commands/tablecmds.c:4435)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (src/backend/tcop/utility.c:1315)
  - [ProcessUtilityForAlterTable](../P/ProcessUtilityForAlterTable.md) (src/backend/tcop/utility.c:1986)

## Notes and Other Information
- Works as a companion function to EventTriggerAlterTableStart to complete ALTER TABLE command collection
- Essential for providing complete object identification in collected ALTER TABLE commands
- The delayed setting of objectId accommodates cases where the relation OID is not immediately available when ALTER TABLE processing begins
- Only operates when event trigger context is active and collection is not inhibited
- Assumes that EventTriggerAlterTableStart has been called previously to establish currentCommand
- Part of the multi-step process for collecting complex ALTER TABLE commands that may contain multiple subcommands