# RememberReplicaIdentityForRebuilding

## Location
src/backend/commands/tablecmds.c: 13689 - 13703

## Overview
RememberReplicaIdentityForRebuilding records replica identity index information when an index needs to be rebuilt during table alterations.

## Definition


## Detailed Description
This function is a utility subroutine used during ALTER TABLE operations that require rebuilding indexes. When an index is marked as a replica identity (used for logical replication to identify rows), this function:

1. **Replica Identity Check**: Verifies if the given index is actually marked as a replica identity using get_index_isreplident()
2. **Uniqueness Validation**: Ensures that only one replica identity index exists per table, as having multiple would be an error condition
3. **Name Storage**: Records the name of the replica identity index in the AlteredTableInfo structure for later restoration

The replica identity index is crucial for logical replication as it provides a way to uniquely identify rows when changes need to be replicated to other PostgreSQL instances. During table rebuilding operations, this information must be preserved and restored after the rebuild completes.

## Parameters / Member Variables
- : OID of the index to check for replica identity status
- : AlteredTableInfo structure where replica identity information is stored

## Dependencies
- Functions called/Symbols referenced:
  - get_index_isreplident
  - get_rel_name
- Called from (representative examples):
  - RememberConstraintForRebuilding
  - RememberIndexForRebuilding

## Notes and Other Information
- Only stores replica identity information if the index is actually marked as a replica identity
- Prevents data corruption by ensuring table has at most one replica identity index
- The stored index name will be used later to restore the replica identity setting after table rebuild
- Essential for maintaining logical replication consistency during schema changes