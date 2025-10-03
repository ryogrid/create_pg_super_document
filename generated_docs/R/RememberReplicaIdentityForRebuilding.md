# RememberReplicaIdentityForRebuilding

## Location
[src/backend/commands/tablecmds.c:13689-13703](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L13689-L13703)

## Overview
RememberReplicaIdentityForRebuilding records replica identity index information when an index needs to be rebuilt during table alterations.

## Definition

```c
static void
RememberReplicaIdentityForRebuilding(Oid indoid, AlteredTableInfo *tab)
```
## Detailed Description
This function is a utility subroutine used during ALTER TABLE operations that require rebuilding indexes. When an index is marked as a replica identity (used for logical replication to identify rows), this function:

1. **Replica Identity Check**: Verifies if the given index is actually marked as a replica identity using get_index_isreplident()
2. **Uniqueness Validation**: Ensures that only one replica identity index exists per table, as having multiple would be an error condition
3. **Name Storage**: Records the name of the replica identity index in the AlteredTableInfo structure for later restoration

The replica identity index is crucial for logical replication as it provides a way to uniquely identify rows when changes need to be replicated to other PostgreSQL instances. During table rebuilding operations, this information must be preserved and restored after the rebuild completes.

## Parameters / Member Variables
- `indoid`: OID of the index to check for replica identity status
- `*tab`: AlteredTableInfo structure where replica identity information is stored
## Dependencies
- Functions called/Symbols referenced:
  - [get_index_isreplident](../g/get_index_isreplident.md)
  - [get_rel_name](../g/get_rel_name.md)
- Called from (representative examples):
  - [RememberConstraintForRebuilding](RememberConstraintForRebuilding.md)
  - [RememberIndexForRebuilding](RememberIndexForRebuilding.md)

## Notes and Other Information
- Only stores replica identity information if the index is actually marked as a replica identity
- Prevents data corruption by ensuring table has at most one replica identity index
- The stored index name will be used later to restore the replica identity setting after table rebuild
- Essential for maintaining logical replication consistency during schema changes

## Simplified Source

```c
static void
RememberReplicaIdentityForRebuilding(Oid indoid, AlteredTableInfo *tab)
{
    // Skip if index is not a replica identity
    if (!get_index_isreplident(indoid))
        return;

    // Ensure only one replica identity index per table
    if (tab->replicaIdentityIndex)
        elog(ERROR, "relation %u has multiple indexes marked as replica identity", tab->relid);

    // Store the replica identity index name for later restoration
    tab->replicaIdentityIndex = get_rel_name(indoid);
}
```