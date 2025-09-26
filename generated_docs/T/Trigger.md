# Trigger

## Location
[src/include/utils/reltrigger.h:23-45](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/reltrigger.h#L23-L45)

## Overview
The Trigger struct represents a database trigger definition containing metadata and configuration information for trigger functions that execute in response to specific database events.

## Definition

```c
typedef struct Trigger
{
	Oid			tgoid;			/* OID of trigger (pg_trigger row) */
	/* Remaining fields are copied from pg_trigger, see pg_trigger.h */
	char	   *tgname;
	Oid			tgfoid;
	int16		tgtype;
	char		tgenabled;
	bool		tgisinternal;
	bool		tgisclone;
	Oid			tgconstrrelid;
	Oid			tgconstrindid;
	Oid			tgconstraint;
	bool		tgdeferrable;
	bool		tginitdeferred;
	int16		tgnargs;
	int16		tgnattr;
	int16	   *tgattr;
	char	  **tgargs;
	char	   *tgqual;
	char	   *tgoldtable;
	char	   *tgnewtable;
} Trigger;
```
## Detailed Description
The Trigger struct is a fundamental data structure that encapsulates all metadata required to define and execute database triggers. This structure is designed to be cleanly included in rel.h and other header files without dependencies, as noted in its comment. Each Trigger instance contains both system catalog information (copied from pg_trigger) and runtime configuration data needed for trigger execution. The structure supports various trigger types including constraint triggers, internal triggers, and cloned triggers with comprehensive configuration options for timing, arguments, and transition table support.

## Parameters / Member Variables
- : OID of the trigger in the pg_trigger system catalog
- : Name of the trigger
- : OID of the trigger function to be executed
- : Trigger type flags indicating timing (BEFORE/AFTER/INSTEAD) and events (INSERT/UPDATE/DELETE/TRUNCATE)
- : Trigger enabled status (enabled, disabled, replica, always)
- : Flag indicating if this is an internal system trigger
- : Flag indicating if this trigger was cloned from a parent table
- : OID of the referenced table for constraint triggers
- : OID of the unique index for constraint triggers
- : OID of the constraint this trigger implements
- : Whether the constraint trigger can be deferred
- : Whether the constraint trigger is initially deferred
- : Number of arguments passed to the trigger function
- : Number of attributes (columns) the trigger is defined on
- : Array of attribute numbers the trigger monitors
- : Array of string arguments passed to the trigger function
- : WHEN clause expression for conditional triggers
- : Name of the OLD transition table (for statement-level triggers)
- : Name of the NEW transition table (for statement-level triggers)

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - RelationBuildTriggers
  - ExecBSInsertTriggers
  - ExecBRInsertTriggers
  - TriggerEnabled
  - AfterTriggerSaveEvent
  - ri_FetchConstraintInfo

## Notes and Other Information
This structure is intentionally separated from trigger.h to maintain clean header dependencies. The fields correspond directly to columns in the pg_trigger system catalog, making it easy to populate from catalog data. The structure supports PostgreSQL's comprehensive trigger system including row-level and statement-level triggers, constraint triggers, and transition tables for complex trigger logic. Memory management of string and array fields (tgname, tgargs, tgattr, etc.) must be handled carefully when copying or freeing Trigger instances.