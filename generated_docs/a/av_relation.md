# av_relation

## Location
[src/backend/postmaster/autovacuum.c:186-193](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/autovacuum.c#L186-L193)

## Overview
The  structure is used by PostgreSQL autovacuum workers to track table-specific information during the first pass of determining which tables need vacuuming or analyzing.

## Definition

```c
typedef struct av_relation
{
	Oid			ar_toastrelid;	/* hash key - must be first */
	Oid			ar_relid;
	bool		ar_hasrelopts;
	AutoVacOpts ar_reloptions;	/* copy of AutoVacOpts from the main table's
								 * reloptions, or NULL if none */
} av_relation;
```
## Detailed Description
The  structure serves as a tracking mechanism for tables and their associated TOAST tables during autovacuum's table discovery and evaluation phase. It maintains the relationship between main tables and their TOAST tables, along with any table-specific autovacuum options that may have been configured. This structure is particularly important for managing TOAST table autovacuum operations, which have special handling requirements in PostgreSQL.

## Parameters / Member Variables
- `ar_toastrelid`: OID of the TOAST table associated with this relation (serves as hash key and must be first field)
- `ar_relid`: OID of the main relation (table) that this entry represents
- `ar_hasrelopts`: Boolean flag indicating whether this table has custom reloptions configured
- `ar_reloptions`: Copy of the AutoVacOpts structure containing table-specific autovacuum configuration options from the main table's reloptions
## Dependencies
- Functions called/Symbols referenced:
  - AutoVacOpts (autovacuum options structure)
- Called from (representative examples):
  - [do_autovacuum](../d/do_autovacuum.md)
  - [table_recheck_autovac](../t/table_recheck_autovac.md)

## Notes and Other Information
- The  field must be positioned first to serve as a proper hash key
- This structure is used primarily during the first pass of autovacuum table evaluation
- Handles the special relationship between main tables and their TOAST tables
- The reloptions field allows for per-table customization of autovacuum behavior
- TOAST tables require special handling because they are automatically created and managed by PostgreSQL