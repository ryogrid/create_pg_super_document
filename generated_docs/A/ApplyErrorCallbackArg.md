# ApplyErrorCallbackArg

## Location
[src/backend/replication/logical/worker.c:219-229](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L219-L229)

## Overview
ApplyErrorCallbackArg is a structure used to save and restore error context information during logical replication apply operations, enabling detailed error reporting with transaction and relation context.

## Definition

```c
typedef struct ApplyErrorCallbackArg
{
	LogicalRepMsgType command;	/* 0 if invalid */
	LogicalRepRelMapEntry *rel;

	/* Remote node information */
	int			remote_attnum;	/* -1 if invalid */
	TransactionId remote_xid;
	XLogRecPtr	finish_lsn;
	char	   *origin_name;
} ApplyErrorCallbackArg;
```
## Detailed Description
ApplyErrorCallbackArg serves as a context container for error reporting during logical replication operations. When errors occur during the application of logical replication changes, this structure provides essential information about the failing operation, including the command type, target relation, remote transaction details, and replication origin. This enables the system to generate informative error messages that help diagnose replication issues by providing context about what operation was being performed and on which relation.

## Parameters / Member Variables
- `command`: LogicalRepMsgType indicating the type of replication command being processed (0 if invalid)
- `*rel`: LogicalRepRelMapEntry pointer to the relation mapping entry for the target relation
- `remote_attnum`: Remote attribute number being processed (-1 if invalid or not applicable)
- `remote_xid`: Transaction ID from the remote/publisher node that generated the change
- `finish_lsn`: XLogRecPtr indicating the LSN where the transaction finished
- `*origin_name`: String containing the name of the replication origin
## Dependencies
- Functions called/Symbols referenced:
  - LogicalRepMsgType
  - [LogicalRepRelMapEntry](../L/LogicalRepRelMapEntry.md)
  - TransactionId
  - XLogRecPtr
- Called from (representative examples):
  - [TransApplyAction](../T/TransApplyAction.md)
  - [apply_error_callback](../a/apply_error_callback.md)

## Notes and Other Information
This structure is primarily used in error callback functions to provide detailed context when logical replication operations fail. The remote node information fields help identify the source of the problematic data, making it easier to debug replication issues. The structure supports partial initialization where some fields may be invalid (-1 for remote_attnum, 0 for command) depending on the context of the error.