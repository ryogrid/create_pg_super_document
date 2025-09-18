# get_schema_sent_in_streamed_txn

## Location
[src/backend/replication/pgoutput/pgoutput.c:1971-1980](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L1971-L1980)

## Overview
This function checks whether schema information has already been sent for a specific relation within a given streamed transaction.

## Definition
```c
static bool
get_schema_sent_in_streamed_txn(RelationSyncEntry *entry, TransactionId xid)
```

## Detailed Description
The `get_schema_sent_in_streamed_txn` function is a utility function used in PostgreSQL's logical replication system to determine if schema information for a particular relation has already been transmitted to downstream subscribers within a specific streamed transaction. This is important for avoiding redundant schema transmissions and ensuring efficient replication. The function operates on the assumption that there will be a relatively small number of streamed transactions, making simple list membership checks efficient.

## Parameters / Member Variables
- `entry`: RelationSyncEntry pointer containing the relation's synchronization metadata, including the list of streamed transactions
- `xid`: TransactionId representing the transaction ID to check for in the streamed transactions list

## Dependencies
- Functions called/Symbols referenced:
  - [list_member_xid](../l/list_member_xid.md)
- Called from (representative examples):
  - [maybe_send_schema](../m/maybe_send_schema.md)

## Notes and Other Information
- This is a simple wrapper function around list_member_xid for better code readability
- The function is designed with the expectation of a relatively small number of streamed transactions, making linear search acceptable
- Used as part of the decision logic in maybe_send_schema to avoid sending duplicate schema information
- The RelationSyncEntry structure maintains a list of transaction IDs (streamed_txns) for which schema has been sent
- Returns true if the transaction ID is found in the relation's streamed transactions list, false otherwise
- This optimization helps reduce network traffic and processing overhead in logical replication by preventing unnecessary schema retransmissions