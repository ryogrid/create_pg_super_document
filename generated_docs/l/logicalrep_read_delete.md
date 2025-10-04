# logicalrep_read_delete

## Location
[src/backend/replication/logical/proto.c:564-585](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/proto.c#L564-L585)

## Overview
Reads a DELETE message from a logical replication stream and populates a tuple data structure with the old tuple information.

## Definition

```c
LogicalRepRelId
logicalrep_read_delete(StringInfo in, LogicalRepTupleData *oldtup)
```
## Detailed Description
This function parses a DELETE operation from the logical replication protocol stream. It extracts the relation ID and validates that the action type is either 'K' (key-only old tuple) or 'O' (full old tuple), then reads the tuple data using the shared tuple reading functionality. The action type depends on the table's replica identity setting - tables with REPLICA_IDENTITY_FULL send the complete old tuple ('O'), while others send only key columns ('K').

The DELETE message format includes:
1. A 4-byte relation ID identifying the target table
2. An action byte ('K' for key-only or 'O' for full old tuple)
3. The actual tuple data in the protocol-specific format

## Parameters / Member Variables
- `in`: StringInfo buffer containing the incoming logical replication stream data
- `*oldtup`: Pointer to LogicalRepTupleData structure that will be filled with the old tuple information
## Dependencies
- Functions called/Symbols referenced:
  - [pq_getmsgint](../p/pq_getmsgint.md) (reads 4-byte integer from message)
  - [pq_getmsgbyte](../p/pq_getmsgbyte.md) (reads single byte from message)
  - [logicalrep_read_tuple](logicalrep_read_tuple.md) (reads tuple data from stream)
- Data types used:
  - LogicalRepRelId (relation identifier type)
  - [LogicalRepTupleData](../L/LogicalRepTupleData.md) (tuple data structure)
- Called from (representative examples):
  - [apply_handle_delete](../a/apply_handle_delete.md) (in logical replication worker)

## Notes and Other Information
- Validates that the action byte is either 'K' (key-only) or 'O' (full tuple) and throws an ERROR for any other value
- The action type reflects the table's replica identity setting at the time of the DELETE
- Part of the logical replication protocol decoder that processes streaming changes
- The relation ID returned helps identify which table the DELETE operation targets
- Located in src/backend/replication/logical/proto.c:564-585

## Simplified Source

```c
LogicalRepRelId logicalrep_read_delete(StringInfo in, LogicalRepTupleData *oldtup) {
    char action;
    LogicalRepRelId relid;

    // Read the relation identifier
    relid = pq_getmsgint(in, 4);

    // Read and validate the action type
    action = pq_getmsgbyte(in);
    if (action != 'K' && action != 'O')
        elog(ERROR, "expected action 'O' or 'K', got %c", action);

    // Read the old tuple data
    logicalrep_read_tuple(in, oldtup);

    return relid;
}
```