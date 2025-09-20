# autovac_table

## Location
[src/backend/postmaster/autovacuum.c:196-207](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/autovacuum.c#L196-L207)

## Overview
The  structure represents a table that has been determined to need vacuuming or analyzing, containing all the necessary parameters and metadata for executing the autovacuum operation.

## Definition

```c
typedef struct autovac_table
{
	Oid			at_relid;
	VacuumParams at_params;
	double		at_storage_param_vac_cost_delay;
	int			at_storage_param_vac_cost_limit;
	bool		at_dobalance;
	bool		at_sharedrel;
	char	   *at_relname;
	char	   *at_nspname;
	char	   *at_datname;
} autovac_table;
```
## Detailed Description
The  structure serves as a comprehensive descriptor for tables that have passed the autovacuum recheck phase and are ready for actual vacuum or analyze operations. Unlike the earlier  structure used for initial table discovery, this structure contains fully resolved vacuum parameters, cost settings, and complete naming information needed to execute the autovacuum operation. It represents the final form of table information after all configuration options, thresholds, and policies have been evaluated and applied.

## Parameters / Member Variables
- : OID of the relation (table) to be vacuumed or analyzed
- : Complete VacuumParams structure containing all vacuum operation parameters
- : Storage parameter for vacuum cost delay, controlling the pacing of vacuum operations
- : Storage parameter for vacuum cost limit, controlling resource usage during vacuum
- : Boolean flag indicating whether vacuum cost balancing should be applied
- : Boolean flag indicating whether this is a shared relation (like system catalogs)
- : Name of the relation as a string
- : Name of the schema/namespace containing this relation
- : Name of the database containing this relation

## Dependencies
- Functions called/Symbols referenced:
  - VacuumParams (vacuum operation parameters structure)
- Called from (representative examples):
  - [do_autovacuum](../d/do_autovacuum.md)
  - [extract_autovac_opts](../e/extract_autovac_opts.md)
  - [table_recheck_autovac](../t/table_recheck_autovac.md)
  - [autovacuum_do_vac_analyze](autovacuum_do_vac_analyze.md)
  - autovac_report_activity

## Notes and Other Information
- This structure represents the final stage of autovacuum table processing, after rechecking and parameter resolution
- Contains both operational parameters and human-readable identifiers for logging and reporting
- The cost delay and limit parameters allow for fine-grained control over vacuum resource usage
- Shared relations require special handling due to their cross-database nature
- Used extensively in the actual vacuum execution phase of autovacuum operations