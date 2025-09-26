# EventTriggerData

## Location
[src/include/commands/event_trigger.h:24-30](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/commands/event_trigger.h#L24-L30)

## Overview
EventTriggerData is a structure that encapsulates information passed to event trigger functions when they are invoked in response to DDL events, login events, or table rewrite events.

## Definition

```c
typedef struct EventTriggerData
{
	NodeTag		type;
	const char *event;			/* event name */
	Node	   *parsetree;		/* parse tree */
	CommandTag	tag;
} EventTriggerData;
```
## Detailed Description
EventTriggerData serves as a parameter structure for event trigger functions in PostgreSQL. Event triggers are special stored procedures that automatically execute in response to specific database events like DDL operations (CREATE, ALTER, DROP), user login events, or table rewrite operations. This structure provides the event trigger function with essential context about the event that caused its invocation.

The structure follows PostgreSQL's standard node structure pattern by including a NodeTag as its first member, allowing it to participate in the node system used throughout the parser and executor. The structure contains the event name, the original parse tree of the command that triggered the event, and a command tag that identifies the specific SQL command type.

## Parameters / Member Variables
- : NodeTag that identifies this as an EventTriggerData node structure
- : String name of the event that triggered the event trigger (e.g., "ddl_command_start", "ddl_command_end", "sql_drop", "login", "table_rewrite")
- : Pointer to the Node representing the parsed SQL command that caused the trigger event; provides access to the full command structure
- : CommandTag enumeration value that identifies the specific type of SQL command (e.g., CMDTAG_CREATE_TABLE, CMDTAG_ALTER_TABLE, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag
  - [Node](../N/Node.md)
  - CommandTag
- Called from (representative examples):
  - [EventTriggerCommonSetup](EventTriggerCommonSetup.md)
  - [EventTriggerDDLCommandStart](EventTriggerDDLCommandStart.md)
  - [EventTriggerDDLCommandEnd](EventTriggerDDLCommandEnd.md)
  - [EventTriggerSQLDrop](EventTriggerSQLDrop.md)
  - [EventTriggerOnLogin](EventTriggerOnLogin.md)
  - [EventTriggerTableRewrite](EventTriggerTableRewrite.md)
  - [EventTriggerInvoke](EventTriggerInvoke.md)
  - CALLED_AS_EVENT_TRIGGER (macro)
  - [plperl_event_trigger_build_args](../p/plperl_event_trigger_build_args.md)
  - [pltcl_event_trigger_handler](../p/pltcl_event_trigger_handler.md)

## Notes and Other Information
- This structure is primarily used in the event trigger system implemented in src/backend/commands/event_trigger.c
- Event triggers are a PostgreSQL extension mechanism that allows custom code to respond to database events
- The parsetree member provides detailed access to the SQL command structure, enabling event triggers to examine and potentially modify the behavior based on specific command details
- Different procedural languages (PL/Perl, PL/Tcl) have specialized functions to build language-specific argument structures from EventTriggerData
- The CALLED_AS_EVENT_TRIGGER macro uses this structure to determine if a function is being called as an event trigger
- Event triggers can be defined for various events including DDL operations, login events, and table rewrites, with this structure providing the necessary context for each event type