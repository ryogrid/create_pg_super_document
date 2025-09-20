# QueryMode

## Location
[src/bin/pgbench/pgbench.c:711-739](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L711-L739)

## Overview
QueryMode is an enumeration that defines the different query execution modes available in pgbench, PostgreSQL's benchmarking tool.

## Definition

```c
struct Command represents one command in a script.
 *
 * lines		The raw, possibly multi-line command text.  Variable substitution
 *				not applied.
 * first_line	A short, single-line extract of 'lines', for error reporting.
 * type			SQL_COMMAND or META_COMMAND
 * meta			The type of meta-command, with META_NONE/GSET/ASET if command
 *				is SQL.
 * argc			Number of arguments of the command, 0 if not yet processed.
 * argv			Command arguments, the first of which is the command or SQL
 *				string itself.  For SQL commands, after post-processing
 *				argv[0] is the same as 'lines' with variables substituted.
 * prepname		The name that this command is prepared under, in prepare mode
 * varprefix	SQL commands terminated with \gset or \aset have this set
 *				to a non NULL value.  If nonempty, it's used to prefix the
 *				variable name that receives the value.
 * aset			do gset on all possible queries of a combined query (\;
```
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
- [Query](Query.md) mode affects performance characteristics significantly, with prepared statements typically offering the best performance for repeated queries
- The QUERYMODE string array provides human-readable names corresponding to each enum value for command-line interface and logging purposes
- Located in src/bin/pgbench/pgbench.c:705-711