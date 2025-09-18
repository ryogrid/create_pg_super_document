# ECPGsetconn

## Location
[src/interfaces/ecpg/ecpglib/connect.c:195-207](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/connect.c#L195-L207)

## Overview
ECPGsetconn sets the current active database connection in ECPG by changing the thread-specific connection to the specified named connection.

## Definition
```c
bool ECPGsetconn(int lineno, const char *connection_name)
```

## Detailed Description
ECPGsetconn is a core ECPG connection management function that switches the current database connection context to a named connection. It retrieves the connection object associated with the given name, validates it through initialization, and then sets it as the active connection for the current thread using pthread thread-specific storage. This allows ECPG programs to work with multiple named database connections and switch between them as needed.

## Parameters / Member Variables
- `lineno`: Line number in the source code where this function is called, used for error reporting and debugging
- `connection_name`: Name of the connection to set as active; must correspond to a previously established connection

## Dependencies
- Functions called/Symbols referenced:
  - ecpg_get_connection
  - [ecpg_init](../e/ecpg_init.md)  
  - pthread_setspecific
- Called from (representative examples):
  - [main](../m/main.md) (in test programs)
  - ECPG-generated code for connection switching

## Notes and Other Information
- Returns true on success, false on failure
- Uses pthread thread-specific storage to maintain per-thread connection state
- The connection must already exist before calling this function
- Part of the ECPG embedded SQL interface for PostgreSQL
- Thread-safe implementation allows multiple threads to have different active connections