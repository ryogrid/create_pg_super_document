# FetchStmt

## Location
[src/include/nodes/parsenodes.h:3328-3335](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3328-L3335)

## Overview
FetchStmt represents the parsed form of SQL FETCH and MOVE statements, which are used to retrieve rows from or advance position in cursors (portals) in PostgreSQL.

## Definition
```c
typedef struct FetchStmt
{
    NodeTag         type;
    FetchDirection  direction;      /* see above */
    long            howMany;        /* number of rows, or position argument */
    char           *portalname;     /* name of portal (cursor) */
    bool            ismove;         /* true if MOVE */
} FetchStmt;
```

## Detailed Description
The FetchStmt structure is a parse tree node that encapsulates all information needed to execute FETCH or MOVE SQL statements. FETCH retrieves rows from a cursor and returns them to the client, while MOVE advances the cursor position without returning data. The statement supports various direction modes (forward, backward, absolute, relative) and can specify the number of rows to fetch or move. Both statements operate on named cursors (portals) that have been previously declared and opened.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a FetchStmt parse node
- `direction`: FetchDirection enum value specifying the direction and method of cursor movement (FORWARD, BACKWARD, ABSOLUTE, etc.)
- `howMany`: Long integer specifying the number of rows to fetch/move, or the absolute position for absolute positioning
- `portalname`: Character string containing the name of the cursor/portal to operate on
- `ismove`: Boolean flag that is true for MOVE statements, false for FETCH statements

## Dependencies
- Functions called/Symbols referenced:
  - [FetchDirection](FetchDirection.md) (enum for cursor movement directions)
  - NodeTag (parse node type identifier)
- Called from (representative examples):
  - [PerformPortalFetch](../P/PerformPortalFetch.md) (portalcmds.c:167)
  - [exec_simple_query](../e/exec_simple_query.md) (postgres.c:1248)
  - [FetchStatementTargetList](FetchStatementTargetList.md) (pquery.c:388)
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (utility.c:703)

## Notes and Other Information
FetchStmt is processed by the utility command execution system and handled primarily by PerformPortalFetch() function. The direction field determines cursor movement behavior: FORWARD/BACKWARD for relative movement, ABSOLUTE for absolute positioning, and RELATIVE for relative positioning. FETCH operations return tuples to the client while MOVE operations only advance the cursor position. Both operations respect cursor scrollability constraints.