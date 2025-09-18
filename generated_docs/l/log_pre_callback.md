# log_pre_callback

## Location
src/bin/psql/startup.c: 92 - 98

## Overview
A static callback function in psql that ensures output streams are flushed before log messages are written.

## Definition


## Detailed Description
This function serves as a pre-logging callback that is called before any log message is output by the PostgreSQL logging system. Its primary purpose is to ensure that any pending output in the query output stream is flushed to prevent log messages from being interleaved with query results. This maintains clean separation between query output and diagnostic messages in psql.

The function checks if there is an active query output stream (pset.queryFout) and if it's different from stdout, it flushes the stream to ensure all buffered output is written before any log message appears.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - fflush (standard C library function)
  - pset.queryFout (global psql settings structure member)
  - stdout (standard output stream)
- Called from (representative examples):
  - main (registered as callback)
  - pg_logging_set_pre_callback
  - pg_log_generic_v

## Notes and Other Information
- This is a static function local to src/bin/psql/startup.c
- It's registered as a pre-logging callback during psql initialization
- The function helps maintain clean output formatting by preventing log messages from appearing in the middle of query results
- Only flushes non-stdout output streams to avoid unnecessary flushing of the primary output