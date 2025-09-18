# EventTriggerCommonSetup

## Location
src/backend/commands/event_trigger.c: 634 - 720

## Overview
Core setup function that prepares event triggers for execution by filtering applicable triggers and building the execution list for a given event and command.

## Definition
```c
static List *EventTriggerCommonSetup(Node *parsetree, EventTriggerEvent event, const char *eventstr, EventTriggerData *trigdata, bool unfiltered)
```

## Detailed Description
This function serves as the central coordination point for event trigger execution setup. It takes a parse tree and event type, determines the appropriate command tag, retrieves cached event triggers for the event, filters them based on command tags and firing rules, and builds a list of trigger function OIDs to execute. The function also populates an EventTriggerData structure with context information for the triggers. It includes comprehensive debug checking to ensure command tags are properly handled across the event trigger system.

## Parameters / Member Variables
- `parsetree`: Parse tree node representing the SQL command being executed
- `event`: EventTriggerEvent enum indicating which type of event is occurring
- `eventstr`: String representation of the event name for trigger data
- `trigdata`: EventTriggerData structure to populate with execution context
- `unfiltered`: If true, bypasses normal filtering and includes all triggers for the event

## Dependencies
- Functions called/Symbols referenced:
  - EventTriggerEvent (event type enumeration)
  - EventTriggerData (trigger execution context structure)
  - CommandTag (command identification type)
  - EventTriggerGetTag (command tag determination)
  - EventCacheLookup (trigger cache retrieval)
  - filter_event_trigger (trigger filtering logic)
  - EventTriggerCacheItem (cached trigger information)
  - command_tag_event_trigger_ok (command tag validation for DDL events)
  - command_tag_table_rewrite_ok (command tag validation for table rewrite events)
  - lappend_oid (list building utility)
- Called from (representative examples):
  - EventTriggerDDLCommandStart
  - EventTriggerDDLCommandEnd
  - EventTriggerSQLDrop
  - EventTriggerOnLogin
  - EventTriggerTableRewrite

## Notes and Other Information
- This is a static internal function, central to the event trigger execution system
- Returns a list of function OIDs to execute, or NIL if no triggers should fire
- Includes extensive debug checking (USE_ASSERT_CHECKING) to validate command tag consistency
- Handles memory management concerns by copying trigger information before catalog access
- Supports both filtered and unfiltered execution modes
- Populates trigdata with T_EventTriggerData type marker and execution context
- Critical performance optimization: fast exit when no triggers are registered for an event
- Ensures command tag validation across different event types (DDL, table rewrite, login)