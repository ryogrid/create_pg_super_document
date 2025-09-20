# IdentifySystemCmd

## Location
[src/include/nodes/replnodes.h:31-34](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/replnodes.h#L31-L34)

## Overview
IdentifySystemCmd is a simple structure representing the IDENTIFY_SYSTEM replication protocol command, used to request system identification information from a PostgreSQL server during replication connections.

## Definition

```c
typedef struct IdentifySystemCmd
{
	NodeTag		type;
} IdentifySystemCmd;
```
## Detailed Description
IdentifySystemCmd is a minimal node structure that represents the IDENTIFY_SYSTEM command in PostgreSQL's replication protocol. This command is typically the first command executed when establishing a replication connection to identify the source system. The structure contains only the standard NodeTag field, as the IDENTIFY_SYSTEM command requires no additional parameters.

When processed by the walsender, this command triggers the IdentifySystem() function which returns a result set containing:
- System identifier (unique database cluster identifier)
- Current timeline ID
- Current WAL position (xlogpos) 
- Database name (if connected to a specific database, NULL otherwise)

This information is essential for replication clients to verify they are connecting to the correct source system and to determine the starting point for replication.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a T_IdentifySystemCmd node type
## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (from nodes/nodes.h)
- Called from (representative examples):
  - walsender.c:2106 - processed in replication command switch statement
  - Triggers IdentifySystem() function execution

## Notes and Other Information
- This is one of the simplest replication command structures, containing only the mandatory NodeTag
- The actual work is performed by the IdentifySystem() function in walsender.c
- Essential for establishing replication connections and verifying system compatibility
- Part of the PostgreSQL streaming replication protocol
- Located in src/include/nodes/replnodes.h alongside other replication command structures