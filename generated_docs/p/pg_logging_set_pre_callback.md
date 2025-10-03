# pg_logging_set_pre_callback

## Location
[src/common/logging.c:193-198](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/logging.c#L193-L198)

## Overview
Sets a callback function to be executed before each log message is output in PostgreSQL's common logging system.

## Definition

```c
void
pg_logging_set_pre_callback(void (*cb) (void))
```
## Detailed Description
This function allows registration of a callback that will be invoked before each log message is formatted and output. The callback function receives no parameters and returns no value. This mechanism is useful for performing setup operations, synchronization, or state management before log output occurs. The callback is stored in the global variable  and will be called by  before processing each log message.

## Parameters / Member Variables
- `*cb`: A function pointer to the callback function to be executed before each log message. The callback takes no parameters and returns void. Pass NULL to clear the callback.
## Dependencies
- Functions called/Symbols referenced:
  - [log_pre_callback](../l/log_pre_callback.md) (global variable assignment)
- Called from (representative examples):
  - [main](../m/main.md) (src/bin/psql/startup.c:134)

## Notes and Other Information
- This is part of PostgreSQL's common logging infrastructure used across multiple components
- The callback is executed by  after level checking but before any actual output formatting
- Only one pre-callback can be registered at a time; setting a new callback overwrites the previous one
- The callback should be lightweight and avoid complex operations that might interfere with logging performance