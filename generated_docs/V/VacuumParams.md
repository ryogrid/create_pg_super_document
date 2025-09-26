# VacuumParams

## Location
[src/include/commands/vacuum.h:217-240](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/commands/vacuum.h#L217-L240)

## Overview
VacuumParams is a configuration structure that customizes the behavior of PostgreSQL's VACUUM and ANALYZE operations by holding various options and parameters.

## Definition

```c
typedef struct VacuumParams
{
	bits32		options;		/* bitmask of VACOPT_* */
	int			freeze_min_age; /* min freeze age, -1 to use default */
	int			freeze_table_age;	/* age at which to scan whole table */
	int			multixact_freeze_min_age;	/* min multixact freeze age, -1 to
											 * use default */
	int			multixact_freeze_table_age; /* multixact age at which to scan
											 * whole table */
	bool		is_wraparound;	/* force a for-wraparound vacuum */
	int			log_min_duration;	/* minimum execution threshold in ms at
									 * which autovacuum is logged, -1 to use
									 * default */
	VacOptValue index_cleanup;	/* Do index vacuum and cleanup */
	VacOptValue truncate;		/* Truncate empty pages at the end */
	Oid			toast_parent;	/* for privilege checks when recursing */

	/*
	 * The number of parallel vacuum workers.  0 by default which means choose
	 * based on the number of indexes.  -1 indicates parallel vacuum is
	 * disabled.
	 */
	int			nworkers;
} VacuumParams;
```
## Detailed Description
VacuumParams serves as the central configuration structure for both VACUUM and ANALYZE operations in PostgreSQL. It encapsulates all customizable parameters that control various aspects of these maintenance operations, including:

- Operation types (vacuum, analyze, or both)
- Transaction age thresholds for freeze operations
- Parallel processing settings
- Index maintenance behavior
- Table truncation behavior
- Logging configuration
- Wraparound prevention settings

The structure is passed through the vacuum/analyze call chain to ensure consistent configuration across all components involved in the maintenance operation. At least one of VACOPT_VACUUM or VACOPT_ANALYZE must be set in the options bitmask.

## Parameters / Member Variables
- : Bitmask of VACOPT_* flags controlling operation behavior
  - VACOPT_VACUUM: Perform vacuum operation
  - VACOPT_ANALYZE: Perform analyze operation
  - VACOPT_VERBOSE: Enable verbose output
  - VACOPT_FREEZE: Force freezing of tuples
  - VACOPT_FULL: Perform full (blocking) vacuum
  - VACOPT_SKIP_LOCKED: Skip tables that cannot be locked
  - VACOPT_PROCESS_MAIN: Process main relation
  - VACOPT_PROCESS_TOAST: Process TOAST table
  - VACOPT_DISABLE_PAGE_SKIPPING: Don't skip any pages
  - VACOPT_SKIP_DATABASE_STATS: Skip database-wide statistics update
  - VACOPT_ONLY_DATABASE_STATS: Only update database-wide statistics
- : Minimum transaction age before tuple can be frozen (-1 for default)
- : Transaction age threshold for full table scan
- : Minimum multixact age for freezing (-1 for default)
- : Multixact age threshold for full table scan
- : Boolean flag forcing wraparound-prevention vacuum
- : Minimum execution time (ms) for autovacuum logging (-1 for default)
- : Index vacuum and cleanup behavior (VACOPTVALUE_UNSPECIFIED/AUTO/DISABLED/ENABLED)
- : Empty page truncation behavior (VACOPTVALUE_UNSPECIFIED/AUTO/DISABLED/ENABLED)
- : Parent table OID for TOAST table privilege checks
- : Number of parallel workers (0=auto, -1=disabled, >0=specific count)

## Dependencies
- Functions called/Symbols referenced:
  - bits32 (bitmask type)
  - VacOptValue (enum for tri-state options)
  - Oid (object identifier type)
  - VACOPT_* constants (option flags)

- Called from (representative examples):
  - ExecVacuum (src/backend/commands/vacuum.c:150)
  - vacuum (src/backend/commands/vacuum.c:479)
  - vacuum_rel (src/backend/commands/vacuum.c:1973)
  - heap_vacuum_rel (src/backend/access/heap/vacuumlazy.c:295)
  - analyze_rel (src/backend/commands/analyze.c:112)
  - autovac_table (src/backend/postmaster/autovacuum.c:199)

## Notes and Other Information
- When adding new VacuumParams members, consider updating vacuumdb utility as well
- The structure supports both manual VACUUM/ANALYZE commands and automatic operations
- Age parameters use transaction ID values to determine when maintenance is needed
- Parallel vacuum workers are automatically chosen based on index count when nworkers=0
- TOAST table processing inherits privileges from the parent table via toast_parent
- VacOptValue enum provides tri-state logic for optional features (unspecified/auto/disabled/enabled)
- The is_wraparound flag indicates emergency vacuum to prevent transaction ID wraparound
- Configuration integrates with autovacuum daemon settings and manual command options