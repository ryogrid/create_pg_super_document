# pg_logging_set_locus_callback

## Location
[src/common/logging.c:199-204](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/logging.c#L199-L204)

## Overview
Sets a callback function to provide source code location information (filename and line number) for log messages in PostgreSQL's common logging system.

## Definition

```c
void
pg_logging_set_locus_callback(void (*cb) (const char **filename, uint64 *lineno))
```
## Detailed Description
This function registers a callback that will be invoked during log message processing to obtain source code location information. The callback receives two output parameters: a pointer to store the filename string and a pointer to store the line number. This mechanism allows the logging system to display contextual information about where log messages originate, which is particularly useful for debugging and development. The callback is stored in the global variable  and is called by  when formatting log output.

## Parameters / Member Variables
- : A function pointer to the callback function that provides location information. The callback receives two parameters:
  - : A pointer to a const char* where the callback should store the filename (can be set to NULL if unavailable)
  - : A pointer to uint64 where the callback should store the line number (can be set to 0 if unavailable)

## Dependencies
- Functions called/Symbols referenced:
  - log_locus_callback (global variable assignment)
- Called from (representative examples):
  - main (src/bin/psql/startup.c:135)

## Notes and Other Information
- This is part of PostgreSQL's common logging infrastructure used across multiple components
- The callback is executed by  after the pre-callback but before message formatting
- Only one locus callback can be registered at a time; setting a new callback overwrites the previous one
- The filename and line number information is used to provide context in log output, showing where the log message originated
- If no callback is set, location information will not be included in log messages