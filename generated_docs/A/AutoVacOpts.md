# AutoVacOpts

## Location
src/include/utils/rel.h: 308 - 326

## Overview
AutoVacOpts is a structure that holds autovacuum-related configuration options for individual relations, allowing per-table customization of autovacuum behavior.

## Definition

```c
typedef struct AutoVacOpts
{
	bool		enabled;
	int			vacuum_threshold;
	int			vacuum_ins_threshold;
	int			analyze_threshold;
	int			vacuum_cost_limit;
	int			freeze_min_age;
	int			freeze_max_age;
	int			freeze_table_age;
	int			multixact_freeze_min_age;
	int			multixact_freeze_max_age;
	int			multixact_freeze_table_age;
	int			log_min_duration;
	float8		vacuum_cost_delay;
	float8		vacuum_scale_factor;
	float8		vacuum_ins_scale_factor;
	float8		analyze_scale_factor;
} AutoVacOpts;
```
## Detailed Description
AutoVacOpts contains autovacuum configuration parameters that can be set on a per-relation basis through relation options (reloptions). These settings override the corresponding global autovacuum configuration parameters for specific tables, allowing fine-grained control over autovacuum behavior.

The structure is part of the relation options system and is embedded within the StdRdOptions structure for heap relations. When autovacuum evaluates whether a relation needs vacuuming or analyzing, it consults these per-relation settings if they exist, falling back to global configuration values otherwise.

The parameters control various aspects of autovacuum behavior including triggering thresholds, cost-based vacuum delay settings, transaction ID freezing behavior, and logging preferences. This granular control is essential for tuning autovacuum behavior for relations with different access patterns and maintenance requirements.

## Parameters / Member Variables
- : Whether autovacuum is enabled for this relation
- : Minimum number of dead tuples needed to trigger vacuum
- : Minimum number of inserted tuples needed to trigger vacuum  
- : Minimum number of changed tuples needed to trigger analyze
- : Cost-based vacuum delay limit for this relation
- : Minimum age of tuples before they can be frozen
- : Maximum age of tuples before they must be frozen
- : Age at which to scan entire table for freezing
- : Minimum age for multixact ID freezing
- : Maximum age for multixact ID freezing  
- : Age at which to scan entire table for multixact freezing
- : Minimum duration to log autovacuum actions (milliseconds)
- : Delay between vacuum cost units (seconds)
- : Scale factor for vacuum threshold calculation
- : Scale factor for insert-triggered vacuum threshold
- : Scale factor for analyze threshold calculation

## Dependencies
- Functions called/Symbols referenced:
  - float8 (PostgreSQL's double precision type)
- Called from (representative examples):
  - extract_autovac_opts (autovacuum worker)
  - default_reloptions (relation options parsing)
  - do_autovacuum (autovacuum main logic)
  - relation_needs_vacanalyze (autovacuum decision making)
  - table_recheck_autovac (autovacuum rechecking)
  - StdRdOptions (embedded as autovacuum member)

## Notes and Other Information
- These options can be set using the WITH clause in CREATE TABLE or ALTER TABLE statements
- Scale factors are multiplied by the relation size to determine actual thresholds
- The structure allows tables with different characteristics to have optimized autovacuum behavior
- Transaction ID freezing parameters are critical for preventing transaction ID wraparound
- Cost-based delay settings help control the I/O impact of autovacuum operations
- Log settings enable per-table control over autovacuum logging verbosity
- This per-relation configuration is essential for managing autovacuum performance in systems with diverse table access patterns