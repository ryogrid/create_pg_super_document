# CommentObject

## Location
[src/backend/commands/comment.c:40-142](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/comment.c#L40-L142)

## Overview
Handles the COMMENT ON SQL command by adding comments to pg_description or pg_shdescription catalog tables for various database objects.

## Definition

```c
ObjectAddress
CommentObject(CommentStmt *stmt)
```
## Detailed Description
CommentObject is the main entry point for processing COMMENT ON SQL statements. It validates the target object, checks permissions, and routes the comment to the appropriate catalog table (pg_description for regular objects or pg_shdescription for cluster-wide objects like databases, tablespaces, and roles).

The function includes special handling for database objects during dump restoration, treating missing databases as warnings rather than errors to prevent pg_restore failures. It also enforces restrictions on column comments, allowing them only on tables, views, materialized views, composite types, foreign tables, and partitioned tables.

## Parameters / Member Variables
- `*stmt`: CommentStmt structure containing the parsed COMMENT ON command with object type, target object specification, and comment text
## Dependencies
- Functions called/Symbols referenced:
  - [get_database_oid](../g/get_database_oid.md): Validates database existence
  - [get_object_address](../g/get_object_address.md): Resolves object specification to ObjectAddress
  - [check_object_ownership](../c/check_object_ownership.md): Verifies user has permission to comment on object
  - [CreateComments](CreateComments.md): Adds comment to pg_description for regular objects
  - [CreateSharedComments](CreateSharedComments.md): Adds comment to pg_shdescription for cluster-wide objects
  - [relation_close](../r/relation_close.md): Closes relation if opened during object resolution
  - [errdetail_relkind_not_supported](../e/errdetail_relkind_not_supported.md): Provides error details for unsupported relation kinds
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md): Main utility command dispatcher
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md): Secondary utility command processor
  - [ATExecCmd](../A/ATExecCmd.md): ALTER TABLE command execution

## Notes and Other Information
- Special case handling for database comments during dump restoration prevents errors from old database names
- Column comments are restricted to specific relation kinds to avoid issues with index column naming changes
- Acquires ShareUpdateExclusiveLock on target objects to prevent concurrent modifications
- Retains locks until transaction commit even after closing relations for concurrency safety

## Simplified Source

```c
ObjectAddress CommentObject(CommentStmt *stmt) {
    Relation relation;
    ObjectAddress address = InvalidObjectAddress;

    // Special case: Handle database comments during dump restoration
    if (stmt->objtype == OBJECT_DATABASE) {
        char *database = strVal(stmt->object);

        if (!OidIsValid(get_database_oid(database, true))) {
            // Warn instead of error to avoid pg_restore failures
            ereport(WARNING, "database \"%s\" does not exist", database);
            return address;
        }
    }

    // Resolve object specification to address and acquire lock
    address = get_object_address(stmt->objtype, stmt->object,
                                &relation, ShareUpdateExclusiveLock, false);

    // Verify user owns the target object
    check_object_ownership(GetUserId(), stmt->objtype, address,
                          stmt->object, relation);

    // Additional validation for column comments
    if (stmt->objtype == OBJECT_COLUMN) {
        // Only allow comments on specific relation types
        if (relation->rd_rel->relkind != RELKIND_RELATION &&
            relation->rd_rel->relkind != RELKIND_VIEW &&
            relation->rd_rel->relkind != RELKIND_MATVIEW &&
            relation->rd_rel->relkind != RELKIND_COMPOSITE_TYPE &&
            relation->rd_rel->relkind != RELKIND_FOREIGN_TABLE &&
            relation->rd_rel->relkind != RELKIND_PARTITIONED_TABLE) {
            ereport(ERROR, "cannot set comment on this relation type");
        }
    }

    // Store comment in appropriate catalog table
    if (stmt->objtype == OBJECT_DATABASE ||
        stmt->objtype == OBJECT_TABLESPACE ||
        stmt->objtype == OBJECT_ROLE) {
        // Cluster-wide objects use shared catalog
        CreateSharedComments(address.objectId, address.classId, stmt->comment);
    } else {
        // Regular objects use standard catalog
        CreateComments(address.objectId, address.classId,
                      address.objectSubId, stmt->comment);
    }

    // Close relation but keep locks until commit
    if (relation != NULL)
        relation_close(relation, NoLock);

    return address;
}
```