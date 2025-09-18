# SetTriggerFlags

## Location
src/backend/commands/trigger.c: 2008 - 2084

## Overview
SetTriggerFlags updates the TriggerDesc's hint flags to include the specified trigger, setting boolean flags that indicate which types of triggers are present for efficient trigger execution planning.

## Definition
static void SetTriggerFlags(TriggerDesc *trigdesc, Trigger *trigger)

## Detailed Description
This function analyzes a single trigger and sets the appropriate boolean hint flags in the TriggerDesc structure to indicate the presence of specific trigger types. The function performs comprehensive trigger type classification by examining the trigger's tgtype field and setting flags for:

1. **Row-level Triggers**: Sets flags for BEFORE, AFTER, and INSTEAD OF triggers on INSERT, UPDATE, and DELETE operations at the row level.

2. **Statement-level Triggers**: Sets flags for BEFORE and AFTER triggers on INSERT, UPDATE, DELETE, and TRUNCATE operations at the statement level.

3. **Transition Table Usage**: Sets flags indicating whether triggers use OLD TABLE or NEW TABLE transition tables for statement-level triggers.

The function uses the TRIGGER_TYPE_MATCHES macro to check if a trigger matches specific combinations of:
- Trigger timing (BEFORE, AFTER, INSTEAD OF)
- Trigger level (ROW, STATEMENT)  
- Trigger event (INSERT, UPDATE, DELETE, TRUNCATE)

These hint flags are used by the trigger execution system to quickly determine which triggers need to be considered for a given operation without having to scan through all triggers.

## Parameters / Member Variables
- : Pointer to TriggerDesc structure whose hint flags will be updated
- : Pointer to Trigger structure containing the trigger definition to analyze

## Dependencies
- Functions called/Symbols referenced:
  - TRIGGER_TYPE_MATCHES: Macro for checking trigger type combinations
  - TRIGGER_FOR_INSERT/UPDATE/DELETE: Macros for checking trigger events
  - TRIGGER_USES_TRANSITION_TABLE: Macro for checking transition table usage
  - Various TRIGGER_TYPE_* constants for trigger classification

- Called from (representative examples):
  - [RelationBuildTriggers](../R/RelationBuildTriggers.md): During trigger descriptor construction

## Notes and Other Information
- This is a static (internal) function used only within the trigger.c module
- The function uses bitwise OR operations (|=) to accumulate trigger type flags
- No row-level TRUNCATE triggers exist in PostgreSQL, so those flags are not set
- Transition table flags are only set for statement-level triggers that actually use transition tables
- The hint flags significantly improve trigger execution performance by avoiding unnecessary trigger scanning during DML operations
- Each trigger type has a corresponding boolean flag in the TriggerDesc structure for O(1) lookup during execution