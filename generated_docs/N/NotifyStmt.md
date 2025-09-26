# NotifyStmt

## Location
[src/include/nodes/parsenodes.h:3622-3627](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3622-L3627)

## Overview
NotifyStmt represents the parsed structure of a NOTIFY SQL statement used for asynchronous inter-session communication in PostgreSQL.

## Definition

```c
typedef struct NotifyStmt
{
	NodeTag		type;
	char	   *conditionname;	/* condition name to notify */
	char	   *payload;		/* the payload string, or NULL if none */
} NotifyStmt;
```
## Detailed Description
NotifyStmt is a parse node that represents the NOTIFY statement in PostgreSQL's SQL grammar. The NOTIFY statement is part of PostgreSQL's asynchronous notification system that allows database sessions to send notifications to other sessions that are listening for specific events. The statement follows the syntax: `NOTIFY channel [, payload]` where channel is an identifier and payload is an optional string message. This mechanism enables efficient inter-session communication without polling.

## Parameters / Member Variables
- `type`: Standard NodeTag identifying this as a NotifyStmt parse node
- `conditionname`: String containing the notification channel name (identifier)
- `payload`: Optional string payload to send with the notification, or NULL if no payload is specified

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (for type identification)
- Called from (representative examples):
  - standard_ProcessUtility (processes the statement via Async_Notify in src/backend/tcop/utility.c:796)
  - ExplainOneUtility (for EXPLAIN support in src/backend/commands/explain.c:587)
  - get_utility_query_def (for query string reconstruction in src/backend/utils/adt/ruleutils.c:7289)
  - PortalRunMulti (as part of query execution in src/backend/tcop/pquery.c:1318)

## Notes and Other Information
- The statement is parsed in gram.y with the rule: `NOTIFY ColId notify_payload`
- NOTIFY can be used both as a standalone command and within rule bodies
- The notification is processed by the Async_Notify function which handles the asynchronous notification system
- Sessions use LISTEN to subscribe to notification channels and UNLISTEN to unsubscribe
- The payload is limited in size and the notification system is designed for lightweight signaling rather than heavy data transfer
- This is part of PostgreSQL's pub/sub messaging system that enables event-driven architectures