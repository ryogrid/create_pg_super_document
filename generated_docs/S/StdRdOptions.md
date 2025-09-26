# StdRdOptions

## Location
[src/include/utils/rel.h:336-346](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/rel.h#L336-L346)

## Overview
StdRdOptions is a structure that defines standard relation options (reloptions) for heap tables, containing configuration parameters that control various aspects of table behavior including storage, autovacuum, and parallel processing.

## Definition

```c
typedef struct StdRdOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int			fillfactor;		/* page fill factor in percent (0..100) */
	int			toast_tuple_target; /* target for tuple toasting */
	AutoVacOpts autovacuum;		/* autovacuum-related options */
	bool		user_catalog_table; /* use as an additional catalog relation */
	int			parallel_workers;	/* max number of parallel workers */
	StdRdOptIndexCleanup vacuum_index_cleanup;	/* controls index vacuuming */
	bool		vacuum_truncate;	/* enables vacuum to truncate a relation */
} StdRdOptions;
```
## Detailed Description
StdRdOptions serves as the standard structure for storing relation options (reloptions) for heap tables in PostgreSQL. This structure is embedded in the rd_options field of relation descriptors and contains parameters that affect how tables are stored, maintained, and accessed. The structure follows the PostgreSQL varlena format with a header that allows it to be stored as variable-length data. These options can be set via CREATE TABLE or ALTER TABLE statements and control various aspects of table behavior from storage efficiency to maintenance operations.

## Parameters / Member Variables
- : Varlena header for variable-length data structure (internal use, should not be modified directly)
- : Page fill factor as a percentage (0-100), controlling how much of each page to fill during INSERT operations
- : Target size for TOAST (The Oversized-Attribute Storage Technique) compression and external storage
- : Structure containing autovacuum-related configuration options such as thresholds and scale factors
- : Boolean flag indicating whether the table should be treated as an additional system catalog table
- : Maximum number of parallel workers that can be used for operations on this table
- : Enum controlling index cleanup behavior during VACUUM operations (AUTO, OFF, or ON)
- : Boolean flag enabling or disabling table truncation during VACUUM operations

## Dependencies
- Functions called/Symbols referenced:
  - [AutoVacOpts](../A/AutoVacOpts.md)
  - [StdRdOptIndexCleanup](StdRdOptIndexCleanup.md)
- Called from (representative examples):
  - [default_reloptions](../d/default_reloptions.md)
  - [heap_reloptions](../h/heap_reloptions.md)
  - [vacuum_rel](../v/vacuum_rel.md)
  - [extract_autovac_opts](../e/extract_autovac_opts.md)
  - RelationGetToastTupleTarget
  - RelationGetFillFactor
  - RelationIsUsedAsCatalogTable
  - RelationGetParallelWorkers

## Notes and Other Information
This structure is the foundation for relation option handling in PostgreSQL and is used by various access methods. The RelationGetFillFactor() and RelationGetTargetPageFreeSpace() functions can only be applied to relations that use this format or a superset for their private options data. The structure is designed to be extensible, allowing different table access methods to define their own option structures that include StdRdOptions as a base.