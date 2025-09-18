# EventTriggerGetTag

## Location
src/backend/commands/event_trigger.c: 620 - 633

## Overview
Determines the appropriate CommandTag for a given parse tree and event trigger event type.

## Definition
```c
static CommandTag EventTriggerGetTag(Node *parsetree, EventTriggerEvent event)
```

## Detailed Description
This function provides a unified way to obtain the CommandTag that represents the current operation for event trigger processing. It handles special cases like login events and delegates to the standard command tag creation mechanism for regular DDL operations. The function bridges the gap between the parse tree representation of commands and the CommandTag system used for event trigger filtering.

## Parameters / Member Variables
- `parsetree`: Parse tree node representing the SQL command being executed
- `event`: EventTriggerEvent enum indicating the type of event trigger event

## Dependencies
- Functions called/Symbols referenced:
  - EventTriggerEvent (event type enumeration)
  - EVT_Login (login event constant)
  - [CreateCommandTag](../C/CreateCommandTag.md) (standard command tag creation function)
  - CMDTAG_LOGIN (login command tag constant)
- Called from (representative examples):
  - [EventTriggerCommonSetup](EventTriggerCommonSetup.md) (called twice in different contexts)

## Notes and Other Information
- This is a static internal function, not exposed outside event_trigger.c
- Provides special handling for login events which don't have a traditional parse tree structure
- For most DDL operations, delegates to the standard CreateCommandTag function
- Part of the event trigger command tag resolution system
- Enables consistent command identification across different event trigger scenarios
- Critical for proper event trigger filtering based on command types