# CreateEventTrigStmt

## Location
[src/include/nodes/parsenodes.h:3029-3036](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3029-L3036)

## Overview
CreateEventTrigStmt represents the parsed structure of a CREATE EVENT TRIGGER SQL statement, used to define database event triggers that fire on DDL events.

## Definition
```c
typedef struct CreateEventTrigStmt
{
	NodeTag		type;
	char	   *trigname;		/* TRIGGER's name */
	char	   *eventname;		/* event's identifier */
	List	   *whenclause;		/* list of DefElems indicating filtering */
	List	   *funcname;		/* qual. name of function to call */
} CreateEventTrigStmt;
```

## Detailed Description
CreateEventTrigStmt is a parse tree node that captures all components of a CREATE EVENT TRIGGER statement. Event triggers are special triggers that fire on database-wide events such as DDL commands, rather than on table-specific DML operations like regular triggers. They allow administrators to monitor and control schema changes, user logins, and other database events. The event trigger function is called with context information about the event that occurred.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a CreateEventTrigStmt node
- `trigname`: Name of the event trigger being created
- `eventname`: String identifying the event type (e.g., "ddl_command_start", "ddl_command_end", "login", "sql_drop", "table_rewrite")
- `whenclause`: Optional list of DefElems specifying filter conditions (e.g., TAG IN ('CREATE TABLE', 'ALTER TABLE'))
- `funcname`: Qualified name of the trigger function to execute (must take no parameters)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for creating the node)
  - [DefElem](../D/DefElem.md) (for when clause conditions)
  - NodeTag (for type identification)
- Called from (representative examples):
  - [CreateEventTrigger](CreateEventTrigger.md) (in src/backend/commands/event_trigger.c:120)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (utility command processing)

## Notes and Other Information
- Part of PostgreSQL's event trigger system for monitoring DDL operations and database events
- Parsed in gram.y rule CreateEventTrigStmt (line 6145) with syntax 'CREATE EVENT TRIGGER name ON event_name [WHEN conditions] EXECUTE FUNCTION function_name()'
- Supported event names include: ddl_command_start, ddl_command_end, login, sql_drop, table_rewrite
- The trigger function must return void and take no parameters
- When clause allows filtering based on event properties like TAG (command type)
- Requires superuser privileges to create
- Event triggers fire for all users and all databases (within the current database for DDL events)
- Processed by CreateEventTrigger function in src/backend/commands/event_trigger.c
- Related to T_CreateEventTrigStmt case in utility command processing