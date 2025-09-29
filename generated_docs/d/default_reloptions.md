# default_reloptions

## Location
[src/backend/access/common/reloptions.c:1847-1916](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/reloptions.c#L1847-L1916)

## Overview
A function that provides standardized parsing of relation options for tables that use the StdRdOptions structure, handling common table storage parameters and autovacuum settings.

## Definition

```c
bytea *
default_reloptions(Datum reloptions, bool validate, relopt_kind kind)
```
## Detailed Description
This function serves as an option parser for any relation type that uses the standard StdRdOptions structure. It defines a comprehensive parsing table that includes storage parameters like fillfactor, toast settings, parallel worker configuration, vacuum settings, and detailed autovacuum options. The function delegates the actual parsing and structure building to build_reloptions, providing it with the StdRdOptions-specific parsing table and structure size.

## Parameters / Member Variables
- : Input Datum containing the raw relation options to be parsed
- : Boolean flag indicating whether to validate all provided options against the parsing table
- : The specific kind of relation options being processed (relopt_kind enum)

## Dependencies
- Functions called/Symbols referenced:
  - relopt_kind (enum type)
  - relopt_parse_elt (struct type)
  - [StdRdOptions](../S/StdRdOptions.md) (struct type)
  - [AutoVacOpts](../A/AutoVacOpts.md) (struct type)
  - RELOPT_TYPE_INT, RELOPT_TYPE_BOOL, RELOPT_TYPE_REAL, RELOPT_TYPE_ENUM (enum values)
  - [build_reloptions](../b/build_reloptions.md) (function)
  - lengthof (macro)
  - offsetof (macro)
- Called from:
  - [heap_reloptions](../h/heap_reloptions.md) (src/backend/access/common/reloptions.c:2036)
  - [heap_reloptions](../h/heap_reloptions.md) (src/backend/access/common/reloptions.c:2047)
  - GET_STRING_RELOPTION (src/include/access/reloptions.h:236)

## Notes and Other Information
- Supports 22 different standard relation options including fillfactor, toast_tuple_target, parallel_workers, user_catalog_table, vacuum_index_cleanup, vacuum_truncate, and 16 autovacuum-related parameters
- The autovacuum options cover thresholds, scale factors, cost parameters, freeze ages for both regular and multixact transactions, and logging settings
- Uses offsetof macro extensively to calculate field positions within nested structures (StdRdOptions containing AutoVacOpts)
- Returns a bytea pointer containing the parsed and structured option data
- This function provides the foundation for standard table option parsing used by heap tables and other relation types that follow the standard options pattern

## Simplified Source

```c
bytea *
default_reloptions(Datum reloptions, bool validate, relopt_kind kind)
{
    // Define parsing table for standard relation options
    static const relopt_parse_elt tab[] = {
        // Storage parameters
        {"fillfactor", RELOPT_TYPE_INT, offsetof(StdRdOptions, fillfactor)},
        {"toast_tuple_target", RELOPT_TYPE_INT, offsetof(StdRdOptions, toast_tuple_target)},
        {"parallel_workers", RELOPT_TYPE_INT, offsetof(StdRdOptions, parallel_workers)},
        {"user_catalog_table", RELOPT_TYPE_BOOL, offsetof(StdRdOptions, user_catalog_table)},

        // Vacuum settings
        {"vacuum_index_cleanup", RELOPT_TYPE_ENUM, offsetof(StdRdOptions, vacuum_index_cleanup)},
        {"vacuum_truncate", RELOPT_TYPE_BOOL, offsetof(StdRdOptions, vacuum_truncate)},

        // Autovacuum boolean settings
        {"autovacuum_enabled", RELOPT_TYPE_BOOL,
         offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, enabled)},

        // Autovacuum threshold settings
        {"autovacuum_vacuum_threshold", RELOPT_TYPE_INT,
         offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, vacuum_threshold)},
        {"autovacuum_vacuum_insert_threshold", RELOPT_TYPE_INT,
         offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, vacuum_ins_threshold)},
        {"autovacuum_analyze_threshold", RELOPT_TYPE_INT,
         offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, analyze_threshold)},

        // Autovacuum cost settings
        {"autovacuum_vacuum_cost_limit", RELOPT_TYPE_INT,
         offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, vacuum_cost_limit)},
        {"autovacuum_vacuum_cost_delay", RELOPT_TYPE_REAL,
         offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, vacuum_cost_delay)},

        // Autovacuum scale factor settings
        {"autovacuum_vacuum_scale_factor", RELOPT_TYPE_REAL,
         offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, vacuum_scale_factor)},
        {"autovacuum_vacuum_insert_scale_factor", RELOPT_TYPE_REAL,
         offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, vacuum_ins_scale_factor)},
        {"autovacuum_analyze_scale_factor", RELOPT_TYPE_REAL,
         offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, analyze_scale_factor)},

        // Autovacuum freeze age settings
        {"autovacuum_freeze_min_age", RELOPT_TYPE_INT,
         offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, freeze_min_age)},
        {"autovacuum_freeze_max_age", RELOPT_TYPE_INT,
         offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, freeze_max_age)},
        {"autovacuum_freeze_table_age", RELOPT_TYPE_INT,
         offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, freeze_table_age)},

        // Autovacuum multixact settings
        {"autovacuum_multixact_freeze_min_age", RELOPT_TYPE_INT,
         offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, multixact_freeze_min_age)},
        {"autovacuum_multixact_freeze_max_age", RELOPT_TYPE_INT,
         offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, multixact_freeze_max_age)},
        {"autovacuum_multixact_freeze_table_age", RELOPT_TYPE_INT,
         offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, multixact_freeze_table_age)},

        // Autovacuum logging
        {"log_autovacuum_min_duration", RELOPT_TYPE_INT,
         offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, log_min_duration)}
    };

    // Build and return parsed options structure
    return (bytea *) build_reloptions(reloptions, validate, kind,
                                      sizeof(StdRdOptions),
                                      tab, lengthof(tab));
}
```