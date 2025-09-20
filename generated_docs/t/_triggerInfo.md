# _triggerInfo

## Location
[src/bin/pg_dump/pg_dump.h:453-459](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L453-L459)

## Overview
The  structure represents PostgreSQL triggers that need to be dumped and restored by pg_dump, including user-defined table triggers.

## Definition

```c
typedef struct _triggerInfo
{
	DumpableObject dobj;
	TableInfo  *tgtable;		/* link to table the trigger is for */
	char		tgenabled;
	bool		tgispartition;
	char	   *tgdef;
} TriggerInfo;
```
## Detailed Description
The  structure is used by pg_dump to manage PostgreSQL triggers, which are functions that are automatically executed in response to certain events on a table or view. The structure stores all necessary metadata about each trigger to enable proper dumping and restoration, including the trigger's enabled status, whether it's associated with a partitioned table, and its complete definition. Triggers are stored in arrays within their associated TableInfo structures and are processed during the dump to generate CREATE TRIGGER statements.

The structure handles both regular table triggers and partition-related triggers, with special consideration for trigger inheritance in partitioned table hierarchies.

## Parameters / Member Variables
- : Base DumpableObject structure containing common dump object metadata (object type DO_TRIGGER, catalog ID, dump ID, name, namespace)
- : Pointer to the TableInfo structure representing the table or view that the trigger is defined on
- : Trigger enabled status character ('O' for origin, 'D' for disabled, 'R' for replica, 'A' for always)
- : Boolean indicating whether this trigger is associated with a partitioned table (affects inheritance behavior)  
- : Complete trigger definition string as retrieved from the database (used to generate CREATE TRIGGER statements)

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (base structure)
  - [TableInfo](../T/TableInfo.md) (for table association)
  
- Called from (representative examples):
  - [getTriggers](../g/getTriggers.md)() (creates TriggerInfo objects by querying pg_trigger system catalog)
  - [dumpTrigger](../d/dumpTrigger.md)() (generates CREATE TRIGGER SQL statements during dump)
  - Sorting functions in pg_dump_sort.c (for proper trigger ordering)

## Notes and Other Information
- The structure is allocated as an array using pg_malloc() in getTriggers()
- Objects of this type have objType set to DO_TRIGGER
- [TriggerInfo](../T/TriggerInfo.md) objects are stored in arrays within TableInfo.triggers, with TableInfo.numTriggers indicating the count
- The tgdef field contains the complete trigger definition retrieved from the database
- Enabled status affects when triggers fire: 'O'=origin only, 'D'=disabled, 'R'=replica only, 'A'=always
- Partition-related triggers (tgispartition=true) may have special inheritance behavior
- Used exclusively within the pg_dump utility for handling PostgreSQL trigger objects
- The trigger definition includes all necessary information to recreate the trigger during restore