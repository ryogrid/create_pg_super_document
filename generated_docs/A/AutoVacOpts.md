# AutoVacOpts

## Location
[src/include/utils/rel.h:308-326](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/rel.h#L308-L326)

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
- `enabled`: Whether autovacuum is enabled for this relation
- `vacuum_threshold`: Minimum number of dead tuples needed to trigger vacuum
- `vacuum_ins_threshold`: Minimum number of inserted tuples needed to trigger vacuum
- `analyze_threshold`: Minimum number of changed tuples needed to trigger analyze
- `vacuum_cost_limit`: Cost-based vacuum delay limit for this relation
- `freeze_min_age`: Minimum age of tuples before they can be frozen
- `freeze_max_age`: Maximum age of tuples before they must be frozen
- `freeze_table_age`: Age at which to scan entire table for freezing
- `multixact_freeze_min_age`: Minimum age for multixact ID freezing
- `multixact_freeze_max_age`: Maximum age for multixact ID freezing
- `multixact_freeze_table_age`: Age at which to scan entire table for multixact freezing
- `log_min_duration`: Minimum duration to log autovacuum actions (milliseconds)
- `vacuum_cost_delay`: Delay between vacuum cost units (seconds)
- `vacuum_scale_factor`: Scale factor for vacuum threshold calculation
- `vacuum_ins_scale_factor`: Scale factor for insert-triggered vacuum threshold
- `analyze_scale_factor`: Scale factor for analyze threshold calculation
## Dependencies
- Functions called/Symbols referenced:
  - float8 (PostgreSQL's double precision type)
- Called from (representative examples):
  - [extract_autovac_opts](../e/extract_autovac_opts.md) (autovacuum worker)
  - [default_reloptions](../d/default_reloptions.md) (relation options parsing)
  - [do_autovacuum](../d/do_autovacuum.md) (autovacuum main logic)
  - [relation_needs_vacanalyze](../r/relation_needs_vacanalyze.md) (autovacuum decision making)
  - [table_recheck_autovac](../t/table_recheck_autovac.md) (autovacuum rechecking)
  - [StdRdOptions](../S/StdRdOptions.md) (embedded as autovacuum member)

## Notes and Other Information
- These options can be set using the WITH clause in CREATE TABLE or ALTER TABLE statements
- Scale factors are multiplied by the relation size to determine actual thresholds
- The structure allows tables with different characteristics to have optimized autovacuum behavior
- Transaction ID freezing parameters are critical for preventing transaction ID wraparound
- Cost-based delay settings help control the I/O impact of autovacuum operations
- Log settings enable per-table control over autovacuum logging verbosity
- This per-relation configuration is essential for managing autovacuum performance in systems with diverse table access patterns