# UnlistenStmt

## Location
[src/include/nodes/parsenodes.h:3643-3647](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3643-L3647)

## Overview
UnlistenStmt represents the parsed structure of an UNLISTEN SQL statement used to unsubscribe from asynchronous notification channels in PostgreSQL.

## Definition

```c
typedef struct UnlistenStmt
{
	NodeTag		type;
	char	   *conditionname;	/* name to unlisten on, or NULL for all */
} UnlistenStmt;
```
## Detailed Description
UnlistenStmt is a parse node that represents the UNLISTEN statement in PostgreSQL's SQL grammar. The UNLISTEN statement is part of PostgreSQL's asynchronous notification system that allows database sessions to unsubscribe from specific notification channels or all channels at once. The statement supports two forms: `UNLISTEN channel` to unsubscribe from a specific channel, and `UNLISTEN *` to unsubscribe from all channels. This provides a way to stop receiving NOTIFY messages that were previously subscribed to via LISTEN statements.

## Parameters / Member Variables
- `type`: Standard NodeTag identifying this as an UnlistenStmt parse node
- `conditionname`: String containing the notification channel name to unsubscribe from, or NULL to unsubscribe from all channels

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (for type identification)
- Called from (representative examples):
  - standard_ProcessUtility (processes the statement via Async_Unlisten or Async_UnlistenAll in src/backend/tcop/utility.c:830)
  - PlannedStmtRequiresSnapshot (for snapshot requirement checking in src/backend/tcop/pquery.c:1748)

## Notes and Other Information
- The statement is parsed in gram.y with two rules: `UNLISTEN ColId` for specific channels and `UNLISTEN '*'` for all channels
- When conditionname is NULL, the statement processes as UNLISTEN * and calls Async_UnlistenAll()
- When conditionname is specified, it calls Async_Unlisten() with the specific channel name
- The statement includes the same restriction checks as other async operations but unlike LISTEN, it can be used to clean up subscriptions
- This is the counterpart to LISTEN in PostgreSQL's pub/sub messaging system
- Sessions automatically unlisten from all channels when they terminate, but explicit UNLISTEN provides fine-grained control over subscriptions