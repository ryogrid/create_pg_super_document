# ListenStmt

## Location
src/include/nodes/parsenodes.h: 3633 - 3637

## Overview
ListenStmt represents the parsed structure of a LISTEN SQL statement used to subscribe to asynchronous notification channels in PostgreSQL.

## Definition

```c
typedef struct ListenStmt
{
	NodeTag		type;
	char	   *conditionname;	/* condition name to listen on */
} ListenStmt;
```
## Detailed Description
ListenStmt is a parse node that represents the LISTEN statement in PostgreSQL's SQL grammar. The LISTEN statement is part of PostgreSQL's asynchronous notification system that allows database sessions to subscribe to specific notification channels. Once a session executes LISTEN on a channel, it will receive all NOTIFY messages sent to that channel. The statement follows the simple syntax: `LISTEN channel` where channel is an identifier. This mechanism enables efficient event-driven communication between database sessions.

## Parameters / Member Variables
- `type`: Standard NodeTag identifying this as a ListenStmt parse node
- `conditionname`: String containing the notification channel name to subscribe to

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (for type identification)
- Called from (representative examples):
  - standard_ProcessUtility (processes the statement via Async_Listen in src/backend/tcop/utility.c:804)
  - PlannedStmtRequiresSnapshot (for snapshot requirement checking in src/backend/tcop/pquery.c:1746)

## Notes and Other Information
- The statement is parsed in gram.y with the rule: `LISTEN ColId`
- LISTEN is processed by the Async_Listen function which registers the session for notifications on the specified channel
- The statement includes restrictions: it cannot be executed within background processes as they have no mechanism to collect NOTIFY messages
- Sessions can use UNLISTEN to unsubscribe from notification channels
- This is part of PostgreSQL's pub/sub messaging system, complementing NOTIFY for asynchronous inter-session communication
- Each session maintains its own set of channels it is listening to, and channels are automatically cleaned up when the session ends