# LogicalSlotInfoArr

## Location
[src/bin/pg_upgrade/pg_upgrade.h:171-186](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/pg_upgrade.h#L171-L186)

## Overview
LogicalSlotInfoArr is a container structure that holds an array of logical replication slot information during the pg_upgrade process, facilitating the migration of logical replication slots from the old to new PostgreSQL cluster.

## Definition

```c
structure represents a relation mapping.
 */
typedef struct
{
	const char *old_tablespace;
	const char *new_tablespace;
	const char *old_tablespace_suffix;
	const char *new_tablespace_suffix;
	Oid			db_oid;
	RelFileNumber relfilenumber;
	/* the rest are used only for logging and error reporting */
	char	   *nspname;		/* namespaces */
	char	   *relname;
} FileNameMap;
```
## Detailed Description
LogicalSlotInfoArr serves as a collection wrapper for logical replication slot metadata during PostgreSQL cluster upgrades. It provides a structured way to manage multiple LogicalSlotInfo entries, enabling pg_upgrade to preserve logical replication configuration across cluster versions. This structure is essential for maintaining logical replication continuity during major version upgrades.

## Parameters / Member Variables
- `nslots`: Integer count of the total number of logical slot information entries in the array
- `slots`: Pointer to an array of LogicalSlotInfo structures containing detailed information about each logical replication slot

## Dependencies
- Functions called/Symbols referenced:
  - LogicalSlotInfo
  - [RelFileNumber](../R/RelFileNumber.md) (indirectly through related structures)
- Called from (representative examples):
  - [check_old_cluster_for_valid_slots](../c/check_old_cluster_for_valid_slots.md)
  - [get_loadable_libraries](../g/get_loadable_libraries.md)  
  - [print_slot_infos](../p/print_slot_infos.md)
  - [create_logical_replication_slots](../c/create_logical_replication_slots.md)

## Notes and Other Information
- This structure is specifically designed for pg_upgrade operations and logical replication slot migration
- The slots array contains detailed information about each slot including name, plugin, two-phase capability, catch-up status, validity, and failover configuration
- Memory management for the slots array should follow standard PostgreSQL memory allocation patterns
- Used in conjunction with slot validation and creation functions during the upgrade process
- Critical for preserving logical replication topology when upgrading PostgreSQL major versions