# AlterEventTrigStmt

## Location
[src/include/nodes/parsenodes.h:3042-3048](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3042-L3048)

## Overview
AlterEventTrigStmt represents the parsed structure of an ALTER EVENT TRIGGER SQL statement, used to modify the firing configuration of existing event triggers.

## Definition
```c
typedef struct AlterEventTrigStmt
{
	NodeTag		type;
	char	   *trigname;		/* TRIGGER's name */
	char		tgenabled;		/* trigger's firing configuration WRT
								 * session_replication_role */
} AlterEventTrigStmt;
```

## Detailed Description
AlterEventTrigStmt is a parse tree node that captures the components of an ALTER EVENT TRIGGER statement. This statement is used to change the firing behavior of an existing event trigger, specifically controlling when the trigger fires in relation to the session_replication_role setting. This is particularly important in replication scenarios where you may want different trigger behavior on the primary server versus replica servers.

## Parameters / Member Variables
- `type`: NodeTag identifying this as an AlterEventTrigStmt node
- `trigname`: Name of the existing event trigger to be modified
- `tgenabled`: Character indicating the new firing configuration (TRIGGER_FIRES_ON_ORIGIN, TRIGGER_FIRES_ALWAYS, TRIGGER_FIRES_ON_REPLICA, or TRIGGER_DISABLED)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for creating the node)
  - TRIGGER_FIRES_* constants (for firing configurations)
  - NodeTag (for type identification)
- Called from (representative examples):
  - [AlterEventTrigger](AlterEventTrigger.md) (in src/backend/commands/event_trigger.c:423)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (utility command processing)

## Notes and Other Information
- Part of PostgreSQL's event trigger system for controlling trigger behavior in replication environments
- Parsed in gram.y rule AlterEventTrigStmt (line 6190) with syntax 'ALTER EVENT TRIGGER name {ENABLE|DISABLE|ENABLE REPLICA|ENABLE ALWAYS}'
- Firing configuration options:
  - TRIGGER_FIRES_ON_ORIGIN ('O'): Fires only when session_replication_role is 'origin' (default)
  - TRIGGER_FIRES_ALWAYS ('A'): Fires regardless of session_replication_role setting
  - TRIGGER_FIRES_ON_REPLICA ('R'): Fires only when session_replication_role is 'replica'
  - TRIGGER_DISABLED ('D'): Never fires
- Requires superuser privileges to execute
- Useful for controlling event trigger behavior during logical replication and maintenance operations
- Processed by AlterEventTrigger function in src/backend/commands/event_trigger.c
- Related to T_AlterEventTrigStmt case in utility command processing