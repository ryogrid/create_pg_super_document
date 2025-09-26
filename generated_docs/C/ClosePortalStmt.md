# ClosePortalStmt

## Location
[src/include/nodes/parsenodes.h:3305-3310](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3305-L3310)

## Overview
ClosePortalStmt represents the parsed form of SQL CLOSE statements, which are used to close cursors (portals) in PostgreSQL.

## Definition
```c
typedef struct ClosePortalStmt
{
    NodeTag     type;
    char       *portalname;     /* name of the portal (cursor) */
    /* NULL means CLOSE ALL */
} ClosePortalStmt;
```

## Detailed Description
The ClosePortalStmt structure is a parse tree node that encapsulates the information needed to execute a CLOSE SQL statement. Cursors in PostgreSQL are implemented as portals, which are named query execution contexts that can be stepped through incrementally. The CLOSE statement is used to explicitly close a cursor and free its associated resources. When the portalname field is NULL, it represents a CLOSE ALL statement that closes all open cursors in the current session.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a ClosePortalStmt parse node
- `portalname`: Character string containing the name of the cursor/portal to close, or NULL for CLOSE ALL

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (parse node type identifier)
- Called from (representative examples):
  - standard_ProcessUtility (utility.c:695)
  - CreateCommandTag (utility.c:2453)

## Notes and Other Information
ClosePortalStmt is processed by the utility command execution system. Cursors are automatically closed at the end of transactions, but explicit closure with CLOSE statements can free resources earlier. The portal management system handles the actual cursor cleanup when this statement is executed. CLOSE ALL is particularly useful for cleaning up multiple cursors at once.