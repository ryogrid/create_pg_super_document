# pg_log

## Location
src/bin/pg_upgrade/util.c: 259 - 269

## Overview
pg_log is a wrapper function for logging messages in PostgreSQL's pg_upgrade utility that provides a variable-argument interface to the internal logging system.

## Definition


## Detailed Description
pg_log serves as a convenient variadic wrapper around pg_log_v, the core logging function in pg_upgrade. It accepts a log message type, a printf-style format string, and variable arguments, then forwards these to pg_log_v for actual processing and output. This function is the primary entry point for most logging operations throughout the pg_upgrade codebase, providing different levels of message output including verbose messages, status updates, reports, warnings, and fatal errors.

## Parameters / Member Variables
- `type`: An eLogType enum value that determines the message level and output behavior (PG_VERBOSE, PG_STATUS, PG_REPORT_NONL, PG_REPORT, PG_WARNING, or PG_FATAL)
- `fmt`: A printf-style format string for the log message
- `...`: Variable arguments corresponding to the format specifiers in the format string

## Dependencies
- Functions called/Symbols referenced:
  - [pg_log_v](pg_log_v.md) (the actual logging implementation)
  - eLogType (enum type for log levels)
- Called from (representative examples):
  - [check_for_data_types_usage](../c/check_for_data_types_usage.md)
  - [output_check_banner](../o/output_check_banner.md)  
  - [report_clusters_compatible](../r/report_clusters_compatible.md)
  - [get_control_data](../g/get_control_data.md)
  - [generate_old_dump](../g/generate_old_dump.md)
  - [connectToServer](../c/connectToServer.md)
  - [executeQueryOrDie](../e/executeQueryOrDie.md)
  - [start_postmaster](../s/start_postmaster.md)
  - [prep_status](prep_status.md)

## Notes and Other Information
- This function is widely used throughout pg_upgrade with over 100 call sites across the codebase
- The function handles variable argument processing using va_list macros before delegating to pg_log_v
- Message formatting and output behavior depends on the eLogType parameter, with different types having different formatting rules and verbosity controls
- Located in src/bin/pg_upgrade/util.c:259-269