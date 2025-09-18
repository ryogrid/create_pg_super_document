# QueryMode

## Location
src/bin/pgbench/pgbench.c: 711 - 739

## Overview
QueryMode is an enumeration that defines the different query execution modes available in pgbench, PostgreSQL's benchmarking tool.

## Definition


## Detailed Description
QueryMode determines how pgbench executes SQL queries during performance testing. The enum provides three distinct execution modes:

- **QUERY_SIMPLE**: Uses PostgreSQL's simple query protocol where SQL text is sent directly to the server for immediate parsing, planning, and execution
- **QUERY_EXTENDED**: Uses PostgreSQL's extended query protocol which separates parsing, binding, and execution phases for better performance with parameterized queries
- **QUERY_PREPARED**: Uses extended query protocol with prepared statements, where queries are prepared once and can be executed multiple times with different parameters for optimal performance

NUM_QUERYMODE serves as a count of available query modes and is used for validation and array sizing.

## Parameters / Member Variables
- : Simple query protocol mode for straightforward SQL execution
- : Extended query protocol mode for parameterized queries
- : Extended query protocol with prepared statements for repeated execution
- : Total count of query modes (not a selectable mode)

## Dependencies
- Functions called/Symbols referenced:
  - QUERY_SIMPLE (used as default value in querymode variable)
- Called from (representative examples):
  - Global variable  initialization at src/bin/pgbench/pgbench.c:713
  - Referenced by QUERYMODE string array for mode name mapping

## Notes and Other Information
- The default query mode is QUERY_SIMPLE as set in the global variable declaration
- Query mode affects performance characteristics significantly, with prepared statements typically offering the best performance for repeated queries
- The QUERYMODE string array provides human-readable names corresponding to each enum value for command-line interface and logging purposes
- Located in src/bin/pgbench/pgbench.c:705-711