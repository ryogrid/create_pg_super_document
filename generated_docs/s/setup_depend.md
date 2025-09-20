# setup_depend

## Location
[src/bin/initdb/initdb.c:1698-1710](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L1698-L1710)

## Overview
The  function configures the PostgreSQL dependency system by stopping the creation of pinned objects during database initialization.

## Definition

```c
static void
setup_depend(FILE *cmdfd)
```
## Detailed Description
This function is responsible for finalizing the setup of PostgreSQL's dependency tracking system () during database cluster initialization. Its primary purpose is to advance the OID (Object Identifier) counter to ensure that subsequently-created objects are not automatically pinned.

Pinned objects in PostgreSQL are system objects that cannot be dropped because they are essential for database operation. During the initial bootstrap phase, many system objects are created as pinned. This function marks the transition point where newly created objects will follow normal dependency rules and can be dropped if needed.

The function executes the SQL command  which internally advances the OID counter and changes the system state to stop creating pinned objects.

## Parameters / Member Variables
- : FILE pointer to the command file descriptor where SQL commands are written for execution

## Dependencies
- Functions called/Symbols referenced:
  - : Macro for writing SQL commands to the command file descriptor
- Called from (representative examples):
  - : Main database initialization sequence
  - : Authentication configuration context

## Notes and Other Information
- This function represents a critical transition point in database initialization where the system moves from bootstrap mode to normal operation mode
- The  function is a PostgreSQL system function that manages the internal OID counter and object pinning state
- Objects created before this function is called will be pinned and cannot be dropped, while objects created after will follow normal dependency rules
- The function is part of the broader initdb process that sets up a new PostgreSQL database cluster
- The double newline (\n\n) in the SQL output provides formatting separation in the generated SQL script