# log_locus_callback

## Location
[src/bin/psql/startup.c:99-114](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/startup.c#L99-L114)

## Overview
A static callback function in psql that provides location information (filename and line number) for log messages when processing SQL input files.

## Definition
```c
static void log_locus_callback(const char **filename, uint64 *lineno)
```

## Detailed Description
This function serves as a locus (location) callback for the PostgreSQL logging system in psql. When log messages are generated, this callback provides context about where the error or message originated by supplying the current input filename and line number being processed. This is particularly useful when psql is executing commands from a file (via \i command or -f option), as it allows error messages to reference the exact location in the input file where the problem occurred.

The function checks if psql is currently processing an input file (pset.inputfile is set) and if so, returns the filename and current line number. If no file is being processed (interactive mode), it returns NULL for filename and 0 for line number.

## Parameters / Member Variables
- `filename`: Double pointer to const char - output parameter that receives the name of the currently processed input file, or NULL if in interactive mode
- `lineno`: Pointer to uint64 - output parameter that receives the current line number in the input file, or 0 if in interactive mode

## Dependencies
- Functions called/Symbols referenced:
  - pset.inputfile (global psql settings structure member)
  - pset.lineno (global psql settings structure member)
- Called from (representative examples):
  - [main](../m/main.md) (registered as callback)
  - [pg_logging_set_locus_callback](../p/pg_logging_set_locus_callback.md)
  - [pg_log_generic_v](../p/pg_log_generic_v.md)

## Notes and Other Information
- This is a static function local to src/bin/psql/startup.c
- It's registered as a locus callback during psql initialization
- The callback enables precise error location reporting when executing SQL scripts
- The function modifies its parameters through pointers to return multiple values
- Line numbers start from 1 when processing files, 0 indicates no file context