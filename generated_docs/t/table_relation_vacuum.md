# table_relation_vacuum

## Location
[src/include/access/tableam.h:1708-1722](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L1708-L1722)

## Overview
A table access method wrapper function that performs regular VACUUM operations on a relation, with the specific actions depending on the individual access method implementation.

## Definition

```c
static inline void
table_relation_vacuum(Relation rel, struct VacuumParams *params,
					  BufferAccessStrategy bstrategy)
```
## Detailed Description
This function provides the interface for performing standard VACUUM operations on relations through the table access method layer. It is designed to handle regular vacuum operations triggered either by user commands or autovacuum processes.

The function delegates vacuum processing to the table access method's specific implementation, allowing different storage engines to optimize vacuum operations according to their storage characteristics. The vacuum operation includes tasks such as removing dead tuples, updating statistics, and managing storage space.

Important operational constraints include that a transaction must already be established before calling this function, and the relation must be locked with ShareUpdateExclusive lock. The function explicitly does not handle VACUUM FULL, CLUSTER, or ANALYZE operations, which have their own specialized pathways.

## Parameters / Member Variables
- : The relation to be vacuumed
- : Vacuum parameters structure containing operation-specific settings and options
- : Buffer access strategy for managing buffer pool usage during the vacuum operation

## Dependencies
- Functions called/Symbols referenced:
  - rel->rd_tableam->relation_vacuum (table access method implementation)
  - [VacuumParams](../V/VacuumParams.md) (vacuum operation parameters structure)
  - [BufferAccessStrategy](../B/BufferAccessStrategy.md) (buffer management strategy)
- Called from (representative examples):
  - [vacuum_rel](../v/vacuum_rel.md) (during regular vacuum operations)

## Notes and Other Information
- Requires an active transaction and ShareUpdateExclusive lock on the relation
- Does not handle VACUUM FULL, CLUSTER, or ANALYZE operations
- Can be triggered by both user commands and autovacuum processes
- The actual vacuum behavior depends entirely on the table access method implementation
- Buffer access strategy helps manage memory usage during large vacuum operations
- This is an inline wrapper function that delegates to the storage engine-specific implementation