# TriggerInfo

## Location
[src/bin/pg_dump/pg_dump.h:460-461](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L460-L461)

## Overview
TriggerInfo is a structure used by pg_dump to store metadata about database triggers during the dump and restore process.

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
TriggerInfo represents trigger metadata in PostgreSQL's pg_dump utility. It extends the base DumpableObject structure to include trigger-specific information necessary for dumping and restoring triggers. The structure maintains references to the associated table and stores the trigger's definition along with its enabled state and partition information.

## Parameters / Member Variables
- `dobj`: Base DumpableObject containing common dump metadata (OID, name, etc.)
- `tgtable`: Pointer to the TableInfo structure representing the table this trigger belongs to
- `tgenabled`: Character indicating the trigger's enabled state (similar to pg_trigger.tgenabled)
- `tgispartition`: Boolean flag indicating whether this is a partition trigger
- `tgdef`: String containing the complete CREATE TRIGGER statement definition

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (base structure)
  - [TableInfo](TableInfo.md) (for table association)
- Called from (representative examples):
  - fmtQualifiedDumpable (src/bin/pg_dump/pg_dump.c:252)
  - [getTriggers](../g/getTriggers.md) (src/bin/pg_dump/pg_dump.c:8232, 8360)
  - [dumpDumpableObject](../d/dumpDumpableObject.md) (src/bin/pg_dump/pg_dump.c:10598)
  - [dumpTrigger](../d/dumpTrigger.md) (src/bin/pg_dump/pg_dump.c:17893)
  - [DOTypeNameCompare](../D/DOTypeNameCompare.md) (src/bin/pg_dump/pg_dump_sort.c:378, 379)

## Notes and Other Information
- This structure is specifically used in the pg_dump utility context for backup and restore operations
- The tgenabled field corresponds to the enabled state stored in the system catalog pg_trigger
- The tgispartition flag helps distinguish between regular triggers and partition-related triggers
- The structure is part of pg_dump's object dependency tracking system for proper dump ordering