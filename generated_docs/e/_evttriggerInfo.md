# _evttriggerInfo

## Location
[src/bin/pg_dump/pg_dump.h:462-470](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L462-L470)

## Overview
The  structure represents PostgreSQL event triggers that need to be dumped and restored by pg_dump, including DDL and other database-level event triggers.

## Definition

```c
typedef struct _evttriggerInfo
{
	DumpableObject dobj;
	char	   *evtname;
	char	   *evtevent;
	const char *evtowner;
	char	   *evttags;
	char	   *evtfname;
	char		evtenabled;
} EventTriggerInfo;
```
## Detailed Description
The  structure is used by pg_dump to manage PostgreSQL event triggers, which are special triggers that fire in response to database-wide events rather than table-specific DML operations. Event triggers were introduced in PostgreSQL 9.3 and can respond to DDL events like CREATE, ALTER, DROP commands, and other database-level operations. The structure stores all necessary metadata about each event trigger to enable proper dumping and restoration, including the triggering event, associated tags, trigger function, and enabled status.

Event triggers operate at the database level and can intercept and respond to schema changes and other global database events, making them useful for auditing, security, and automated database management tasks.

## Parameters / Member Variables
- : Base DumpableObject structure containing common dump object metadata (object type DO_EVENT_TRIGGER, catalog ID, dump ID, name, namespace)
- : Name of the event trigger (duplicated from dobj.name for convenience)
- : Event that triggers the function (e.g., 'ddl_command_start', 'ddl_command_end', 'table_rewrite', 'sql_drop')
- : Name of the role (user) that owns the event trigger
- : Tag filter string that specifies which command tags can fire the trigger (NULL if no filter)
- : Name of the function that is executed when the event trigger fires
- : Event trigger enabled status character ('O' for origin, 'D' for disabled, 'R' for replica, 'A' for always)

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (base structure)
  
- Called from (representative examples):
  - [getEventTriggers](../g/getEventTriggers.md)() (creates EventTriggerInfo objects by querying pg_event_trigger system catalog)
  - [dumpEventTrigger](../d/dumpEventTrigger.md)() (generates CREATE EVENT TRIGGER SQL statements during dump)
  - [selectDumpableObject](../s/selectDumpableObject.md)() (determines if object should be dumped)

## Notes and Other Information
- Event triggers were introduced in PostgreSQL 9.3, so this structure is only used when dumping from servers with version 90300 or higher
- The structure is allocated as an array using pg_malloc() in getEventTriggers()
- Objects of this type have objType set to DO_EVENT_TRIGGER
- Event types include: 'ddl_command_start', 'ddl_command_end', 'table_rewrite', 'sql_drop'
- The evttags field can contain a list of command tags that are allowed to fire the trigger, or be NULL for no restrictions
- Enabled status affects when event triggers fire: 'O'=origin only, 'D'=disabled, 'R'=replica only, 'A'=always
- Used exclusively within the pg_dump utility for handling PostgreSQL event trigger objects
- Event triggers are global to the database and not associated with specific tables or schemas