# RememberClusterOnForRebuilding

## Location
[src/backend/commands/tablecmds.c:13704-13719](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L13704-L13719)

## Overview
RememberClusterOnForRebuilding records clustered index information when an index needs to be rebuilt during table alterations.

## Definition

```c
static void
RememberClusterOnForRebuilding(Oid indoid, AlteredTableInfo *tab)
```
## Detailed Description
This function is a utility subroutine used during ALTER TABLE operations that require rebuilding indexes. When an index is marked as clustered (meaning the table data is physically organized according to the index order), this function:

1. **Cluster Status Check**: Verifies if the given index is actually marked as clustered using get_index_isclustered()
2. **Uniqueness Validation**: Ensures that only one clustered index exists per table, as having multiple would be an error condition
3. **Name Storage**: Records the name of the clustered index in the AlteredTableInfo structure for later restoration

A clustered index indicates that the table's physical storage order matches the index order, which can provide performance benefits for certain queries. During table rebuilding operations, this clustering information must be preserved and restored after the rebuild completes to maintain the intended storage organization.

## Parameters / Member Variables
- : OID of the index to check for clustered status
- : AlteredTableInfo structure where clustered index information is stored

## Dependencies
- Functions called/Symbols referenced:
  - [get_index_isclustered](../g/get_index_isclustered.md)
  - [get_rel_name](../g/get_rel_name.md)
- Called from (representative examples):
  - [RememberConstraintForRebuilding](RememberConstraintForRebuilding.md)
  - [RememberIndexForRebuilding](RememberIndexForRebuilding.md)

## Notes and Other Information
- Only stores cluster information if the index is actually marked as clustered
- Prevents data corruption by ensuring table has at most one clustered index
- The stored index name will be used later to restore the clustering setting after table rebuild
- Essential for maintaining intended physical storage organization during schema changes