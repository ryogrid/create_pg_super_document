# get_explain_guc_options

## Location
[src/backend/utils/misc/guc.c:5339-5439](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L5339-L5439)

## Overview
Returns an array of GUC configuration parameters that are relevant to query planning and have been modified from their default values, specifically for display in EXPLAIN output.

## Definition
```c
struct config_generic **get_explain_guc_options(int *num)
```

## Detailed Description
This function identifies and returns GUC parameters that should be included in EXPLAIN plan output to help users understand how configuration settings affected query planning. The function implements a selective filtering process:

1. **Flag Filtering**: Only considers parameters marked with the `GUC_EXPLAIN` flag, indicating they are relevant to query planning
2. **Visibility Check**: Ensures the current user has permission to view each parameter using `ConfigOptionIsVisible()`
3. **Modification Detection**: Compares current values against boot/default values for each GUC type (bool, int, real, string, enum)
4. **Non-default Source**: Only examines parameters from the `guc_nondef_list` (those with sources other than `PGC_S_DEFAULT`)

The function handles all GUC data types and performs type-specific value comparisons to determine if a parameter has been modified from its built-in default.

## Parameters / Member Variables
- `num`: Output parameter that receives the count of GUC options included in the returned array

## Dependencies
- Functions called/Symbols referenced:
  - `palloc` - Allocates memory for the result array
  - `hash_get_num_entries` - Gets total GUC count for array sizing
  - `dlist_foreach` - Iterates through the non-default GUC list
  - `dlist_container` - Extracts config_generic from list node
  - `ConfigOptionIsVisible` - Checks parameter visibility permissions
  - `strcmp` - Compares string values for modification detection
  - `elog` - Reports unexpected GUC types as errors
- Data structures used:
  - `config_generic` - Base GUC configuration structure
  - `config_bool`, `config_int`, `config_real`, `config_string`, `config_enum` - Type-specific GUC structures
  - `dlist_iter` - Iterator for doubly-linked list traversal
  - `guc_nondef_list` - List of non-default GUC parameters
- GUC type constants:
  - `PGC_BOOL`, `PGC_INT`, `PGC_REAL`, `PGC_STRING`, `PGC_ENUM` - GUC variable types
- Called from (representative examples):
  - `ExplainPrintSettings` - Displays relevant GUC settings in EXPLAIN output

## Notes and Other Information
- The function pre-allocates an array sized for all GUC variables, though typically only a small fraction will be included (those marked `GUC_EXPLAIN`)
- Only parameters marked with `GUC_EXPLAIN` are considered - this flag is set on planning-related parameters like `enable_seqscan`, `work_mem`, etc.
- String comparison handles NULL values carefully - both NULL is considered unmodified, one NULL is considered modified
- The function supports PostgreSQL's role-based parameter visibility system - some parameters may be hidden from non-superusers
- This is specifically designed for EXPLAIN output to show which non-default settings influenced the query plan
- The boot_val comparison ensures only truly modified parameters are shown, not just those set explicitly to their default values