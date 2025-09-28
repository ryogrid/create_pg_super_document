# log_pre_callback

## Location
[src/bin/psql/startup.c:92-98](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/startup.c#L92-L98)

## Overview
A static callback function in psql that ensures output streams are flushed before log messages are written.

## Definition

```c
static void
log_pre_callback(void)
```
## Detailed Description
This function serves as a pre-logging callback that is called before any log message is output by the PostgreSQL logging system. Its primary purpose is to ensure that any pending output in the query output stream is flushed to prevent log messages from being interleaved with query results. This maintains clean separation between query output and diagnostic messages in psql.

The function checks if there is an active query output stream (pset.queryFout) and if it's different from stdout, it flushes the stream to ensure all buffered output is written before any log message appears.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - fflush (standard C library function)
  - pset.queryFout (global psql settings structure member)
  - stdout (standard output stream)
- Called from (representative examples):
  - [main](../m/main.md) (registered as callback)
  - [pg_logging_set_pre_callback](../p/pg_logging_set_pre_callback.md)
  - [pg_log_generic_v](../p/pg_log_generic_v.md)

## Notes and Other Information
- This is a static function local to src/bin/psql/startup.c
- It's registered as a pre-logging callback during psql initialization
- The function helps maintain clean output formatting by preventing log messages from appearing in the middle of query results
- Only flushes non-stdout output streams to avoid unnecessary flushing of the primary output

## Simplified Source

```c
// Simplified version of log_pre_callback
static void log_pre_callback(void) {
    // Flush query output stream if it's not stdout
    // This ensures clean separation between query results and log messages
    if (pset.queryFout && pset.queryFout != stdout) {
        fflush(pset.queryFout);
    }
}
```

Key simplifications made:
- Added explanatory comments to clarify the purpose
- Maintained the original logic as it's already quite simple and essential
- Enhanced readability with clear comment explaining the condition