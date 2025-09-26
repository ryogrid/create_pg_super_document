# logicalrep_read_update

## Location
[src/backend/replication/logical/proto.c:492-532](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/proto.c#L492-L532)

## Overview
Reads an UPDATE message from a logical replication stream and populates tuple data structures with both old and new tuple information.

## Definition

```c
LogicalRepRelId
logicalrep_read_update(StringInfo in, bool *has_oldtuple,
					   LogicalRepTupleData *oldtup,
					   LogicalRepTupleData *newtup)
```
## Detailed Description
This function parses an UPDATE operation from the logical replication protocol stream. It handles the variable format of UPDATE messages which may or may not include old tuple data depending on the table's replica identity setting. The function processes:

1. The relation ID identifying the target table
2. Action bytes indicating tuple types ('K' for key-only old tuple, 'O' for full old tuple, 'N' for new tuple)
3. Optional old tuple data (when action is 'K' or 'O')
4. Required new tuple data (action 'N')

The function validates the action sequence and sets the has_oldtuple flag to indicate whether old tuple data was present in the stream.

## Parameters / Member Variables
- : StringInfo buffer containing the incoming logical replication stream data
- : Pointer to boolean flag that will be set to indicate if old tuple data was read
- : Pointer to LogicalRepTupleData structure for old tuple values (populated if has_oldtuple becomes true)
- : Pointer to LogicalRepTupleData structure for new tuple values (always populated)

## Dependencies
- Functions called/Symbols referenced:
  - pq_getmsgint (reads 4-byte integer from message)
  - pq_getmsgbyte (reads single byte from message)
  - logicalrep_read_tuple (reads tuple data from stream)
- Data types used:
  - LogicalRepRelId (relation identifier type)
  - LogicalRepTupleData (tuple data structure)
- Called from (representative examples):
  - apply_handle_update (in logical replication worker)

## Notes and Other Information
- Validates action bytes: 'K' (key-only old tuple), 'O' (full old tuple), 'N' (new tuple)
- The presence of old tuple data depends on the table's replica identity setting
- Throws ERROR for invalid action byte sequences
- Always expects a new tuple ('N' action) as the final component of an UPDATE message
- Part of the logical replication protocol decoder for processing streaming changes
- Located in src/backend/replication/logical/proto.c:492-532