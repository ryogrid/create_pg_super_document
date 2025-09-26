# ReplicaIdentityStmt

## Location
src/include/nodes/parsenodes.h: 2419 - 2424

## Overview
ReplicaIdentityStmt represents an ALTER TABLE ... REPLICA IDENTITY statement that configures how rows are identified for logical replication purposes.

## Definition
```c
typedef struct ReplicaIdentityStmt
{
    NodeTag     type;
    char        identity_type;    /* replica identity type */
    char       *name;            /* index name for INDEX type */
} ReplicaIdentityStmt;
```

## Detailed Description
ReplicaIdentityStmt represents the ALTER TABLE ... REPLICA IDENTITY command, which sets the replica identity for a table used in logical replication. The replica identity determines which columns are included in the write-ahead log (WAL) records for UPDATE and DELETE operations, allowing logical replication to identify and replicate these changes accurately.

The statement supports four different replica identity types: DEFAULT (using primary key if available), FULL (all columns), NOTHING (no replica identity), and INDEX (using a specific unique index). When INDEX type is specified, the referenced index must be unique, immediate (not deferred), and contain only non-nullable columns. Expression and partial indexes are not allowed.

The execution performs extensive validation to ensure the specified index meets all requirements for serving as a replica identity, including uniqueness constraints, nullability checks, and system column restrictions.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a ReplicaIdentityStmt node
- `identity_type`: Character indicating the replica identity type ('d'=DEFAULT, 'f'=FULL, 'n'=NOTHING, 'i'=INDEX)
- `name`: Name of the index to use when identity_type is REPLICA_IDENTITY_INDEX (NULL for other types)

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (inherited node type system)
  - REPLICA_IDENTITY_* constants (for identity type values)
- Called from (representative examples):
  - ATExecReplicaIdentity (main execution function for ALTER TABLE context)
  - ATExecCmd (ALTER TABLE command dispatcher)
  - child_dependency_type (for inheritance handling)

## Notes and Other Information
- Only valid as part of ALTER TABLE statement, not standalone
- REPLICA_IDENTITY_DEFAULT ('d'): Uses primary key, falls back to unique index, then full row
- REPLICA_IDENTITY_FULL ('f'): Includes all column values in WAL records
- REPLICA_IDENTITY_NOTHING ('n'): No replica identity, UPDATE/DELETE not replicated
- REPLICA_IDENTITY_INDEX ('i'): Uses specified unique index for identification
- Index-based replica identity requires unique, immediate, non-partial, non-expression index
- All indexed columns must be NOT NULL for INDEX type
- Critical for logical replication and change data capture functionality
- System columns cannot be part of replica identity indexes