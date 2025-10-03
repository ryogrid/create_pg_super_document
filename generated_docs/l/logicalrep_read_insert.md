# logicalrep_read_insert

## Location
[src/backend/replication/logical/proto.c:436-457](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/proto.c#L436-L457)

## Overview
Reads an INSERT message from a logical replication stream and populates a tuple data structure with the new tuple information.

## Definition

```c
LogicalRepRelId
logicalrep_read_insert(StringInfo in, LogicalRepTupleData *newtup)
```
## Detailed Description
This function parses an INSERT operation from the logical replication protocol stream. It extracts the relation ID and validates that the action type is 'N' (new tuple), then reads the tuple data using the shared tuple reading functionality. The function is part of PostgreSQL's logical replication protocol implementation, which enables streaming of database changes to subscribers.

The function follows the logical replication wire protocol format where INSERT messages contain:
1. A 4-byte relation ID identifying the target table
2. An action byte ('N' for new tuple in INSERT operations)
3. The actual tuple data in the protocol-specific format

## Parameters / Member Variables
- `in`: StringInfo buffer containing the incoming logical replication stream data
- `*newtup`: Pointer to LogicalRepTupleData structure that will be filled with the new tuple information
## Dependencies
- Functions called/Symbols referenced:
  - [pq_getmsgint](../p/pq_getmsgint.md) (reads 4-byte integer from message)
  - [pq_getmsgbyte](../p/pq_getmsgbyte.md) (reads single byte from message)
  - [logicalrep_read_tuple](logicalrep_read_tuple.md) (reads tuple data from stream)
- Data types used:
  - LogicalRepRelId (relation identifier type)
  - [LogicalRepTupleData](../L/LogicalRepTupleData.md) (tuple data structure)
- Called from (representative examples):
  - [apply_handle_insert](../a/apply_handle_insert.md) (in logical replication worker)

## Notes and Other Information
- The function validates the action byte must be 'N' (new tuple) and throws an ERROR if any other value is encountered
- This is part of the logical replication protocol decoder that processes streaming changes
- The relation ID returned helps identify which table the INSERT operation targets
- Located in src/backend/replication/logical/proto.c:436-457