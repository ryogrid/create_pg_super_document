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

## Simplified Source

```c
static void
ATExecReplicaIdentity(Relation rel, ReplicaIdentityStmt *stmt, LOCKMODE lockmode)
{
    Oid indexOid;
    Relation indexRel;
    int key;

    // Handle simple cases: DEFAULT, FULL, NOTHING
    if (stmt->identity_type == REPLICA_IDENTITY_DEFAULT ||
        stmt->identity_type == REPLICA_IDENTITY_FULL ||
        stmt->identity_type == REPLICA_IDENTITY_NOTHING)
    {
        relation_mark_replica_identity(rel, stmt->identity_type, InvalidOid, true);
        return;
    }
    else if (stmt->identity_type != REPLICA_IDENTITY_INDEX)
    {
        elog(ERROR, "unexpected identity type %u", stmt->identity_type);
    }

    // For USING INDEX, validate the specified index
    indexOid = get_relname_relid(stmt->name, rel->rd_rel->relnamespace);
    if (!OidIsValid(indexOid))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                       errmsg("index \"%s\" for table \"%s\" does not exist",
                              stmt->name, RelationGetRelationName(rel))));

    indexRel = index_open(indexOid, ShareLock);

    // Verify index belongs to this table
    if (indexRel->rd_index == NULL ||
        indexRel->rd_index->indrelid != RelationGetRelid(rel))
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                       errmsg("\"%s\" is not an index for table \"%s\"",
                              RelationGetRelationName(indexRel),
                              RelationGetRelationName(rel))));

    // Validate index characteristics for replica identity
    if (!indexRel->rd_indam->amcanunique || !indexRel->rd_index->indisunique)
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                       errmsg("cannot use non-unique index \"%s\" as replica identity",
                              RelationGetRelationName(indexRel))));

    if (!indexRel->rd_index->indimmediate)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("cannot use non-immediate index \"%s\" as replica identity",
                              RelationGetRelationName(indexRel))));

    if (RelationGetIndexExpressions(indexRel) != NIL)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("cannot use expression index \"%s\" as replica identity",
                              RelationGetRelationName(indexRel))));

    if (RelationGetIndexPredicate(indexRel) != NIL)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("cannot use partial index \"%s\" as replica identity",
                              RelationGetRelationName(indexRel))));

    // Check all index columns are NOT NULL and not system columns
    for (key = 0; key < IndexRelationGetNumberOfKeyAttributes(indexRel); key++)
    {
        int16 attno = indexRel->rd_index->indkey.values[key];
        Form_pg_attribute attr;

        if (attno <= 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                           errmsg("index \"%s\" cannot be used as replica identity because column %d is a system column",
                                  RelationGetRelationName(indexRel), attno)));

        attr = TupleDescAttr(rel->rd_att, attno - 1);
        if (!attr->attnotnull)
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                           errmsg("index \"%s\" cannot be used as replica identity because column \"%s\" is nullable",
                                  RelationGetRelationName(indexRel), NameStr(attr->attname))));
    }

    // Index is suitable - mark it as replica identity
    relation_mark_replica_identity(rel, stmt->identity_type, indexOid, true);
    index_close(indexRel, NoLock);
}
```