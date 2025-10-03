# UnsyncVariables

## Location
[src/bin/psql/command.c:4083-4102](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L4083-L4102)

## Overview
Clears connection-specific psql variables when there is no active database connection, ensuring that stale connection information is not retained.

## Definition
```c
void UnsyncVariables(void)
```

## Detailed Description
This function serves as the counterpart to SyncVariables(), cleaning up connection-specific variables when a database connection is lost or explicitly disconnected. It systematically sets all connection-related psql variables to NULL, preventing the retention of outdated connection information that could be misleading or cause errors.

The function is called whenever psql needs to reflect a disconnected state, ensuring that scripts and user queries cannot accidentally reference connection parameters from a previous session.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [SetVariable](../S/SetVariable.md)() (multiple calls to clear variables)
- Called from:
  - [do_connect](../d/do_connect.md) (at src/bin/psql/command.c:3773)
  - [CheckConnection](../C/CheckConnection.md) (at src/bin/psql/common.c:373)

## Notes and Other Information
- Clears the following psql variables by setting them to NULL: DBNAME, USER, HOST, PORT, ENCODING, SERVER_VERSION_NAME, SERVER_VERSION_NUM
- Called when database connections are terminated or when connection attempts fail
- Ensures clean state management by preventing access to stale connection information
- Works in conjunction with SyncVariables() to maintain accurate connection state representation
- Essential for proper variable management in interactive psql sessions where users may connect and disconnect from multiple databases