# ATExecReplicaIdentity

## Location
[src/backend/commands/tablecmds.c:16760-16867](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L16760-L16867)

## Overview
Implements the `ALTER TABLE REPLICA IDENTITY` command by validating and setting the table's replica identity configuration for logical replication.

## Definition
```c
static void ATExecReplicaIdentity(Relation rel, ReplicaIdentityStmt *stmt, LOCKMODE lockmode)
```

## Detailed Description
ATExecReplicaIdentity handles the `ALTER TABLE REPLICA IDENTITY` SQL command, which configures how PostgreSQL identifies rows for logical replication purposes. The function supports four replica identity types:

1. **DEFAULT**: Uses the primary key (if any) as the replica identity
2. **FULL**: Uses all columns as the replica identity  
3. **NOTHING**: No replica identity (disables logical replication for the table)
4. **USING INDEX**: Uses a specified index as the replica identity

For the first three types, the function directly calls relation_mark_replica_identity. For USING INDEX, it performs extensive validation to ensure the index is suitable:

- Index must exist and belong to the target table
- Index must be unique and support uniqueness
- Index must be immediate (not deferred)
- Index cannot be an expression index
- Index cannot be a partial index
- All indexed columns must be NOT NULL
- System columns are not permitted

The validation ensures the index can reliably identify rows for replication, maintaining data consistency across logical replication subscribers.

## Parameters / Member Variables
- `rel`: The relation being altered
- `stmt`: The parsed ALTER TABLE REPLICA IDENTITY statement containing identity type and index name
- `lockmode`: Lock mode for the operation (parameter present but not actively used)

## Dependencies
- Functions called/Symbols referenced:
  - [relation_mark_replica_identity](../r/relation_mark_replica_identity.md): Updates the replica identity configuration
  - [get_relname_relid](../g/get_relname_relid.md): Resolves index name to OID within the table's namespace
  - [index_open](../i/index_open.md): Opens the specified index with ShareLock
  - [index_close](../i/index_close.md): Closes the index relation
  - [RelationGetIndexExpressions](../R/RelationGetIndexExpressions.md): Checks for expression indexes
  - [RelationGetIndexPredicate](../R/RelationGetIndexPredicate.md): Checks for partial indexes
  - IndexRelationGetNumberOfKeyAttributes: Gets count of key attributes
  - [ReplicaIdentityStmt](../R/ReplicaIdentityStmt.md): Structure containing parsed statement information
  - REPLICA_IDENTITY_DEFAULT/FULL/NOTHING/INDEX: Constants for identity types

- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md): Main ALTER TABLE command execution dispatcher

## Notes and Other Information
- The function performs comprehensive validation only for REPLICA_IDENTITY_INDEX type
- All validation errors use appropriate SQL error codes for proper client feedback
- The ShareLock on the index prevents concurrent modifications during validation
- System column rejection protects against using internal PostgreSQL columns
- NOT NULL requirement ensures reliable row identification across all scenarios
- The function assumes the caller holds appropriate locks on the target relation
- Expression and partial indexes are rejected due to replication complexity
- Deferred uniqueness constraints cannot guarantee immediate uniqueness needed for replication