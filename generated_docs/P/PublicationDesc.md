# PublicationDesc

## Location
[src/include/catalog/pg_publication.h:80-98](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/catalog/pg_publication.h#L80-L98)

## Overview
PublicationDesc is a structure that extends PublicationActions with additional validation flags for row filters and column lists in logical replication publications.

## Definition

```c
typedef struct PublicationDesc
{
	PublicationActions pubactions;

	/*
	 * true if the columns referenced in row filters which are used for UPDATE
	 * or DELETE are part of the replica identity or the publication actions
	 * do not include UPDATE or DELETE.
	 */
	bool		rf_valid_for_update;
	bool		rf_valid_for_delete;

	/*
	 * true if the columns are part of the replica identity or the publication
	 * actions do not include UPDATE or DELETE.
	 */
	bool		cols_valid_for_update;
	bool		cols_valid_for_delete;
} PublicationDesc;
```
## Detailed Description
PublicationDesc is a comprehensive descriptor structure for logical replication publications that combines operation controls with validation state for advanced replication features. It extends the basic PublicationActions with additional boolean flags that track the validity of row filters and column lists for UPDATE and DELETE operations.

This structure is essential for ensuring data consistency and proper replication behavior when publications use row filters or column lists. The validation flags help determine whether the specified filters and column selections are compatible with the table's replica identity, which is crucial for correctly identifying and replicating row changes.

The structure is used primarily in the relation cache system to store precomputed validation results, avoiding repeated validation checks during replication operations.

## Parameters / Member Variables
- `pubactions`: PublicationActions structure containing the basic operation flags (INSERT, UPDATE, DELETE, TRUNCATE)
- `rf_valid_for_update`: Boolean indicating whether row filters are valid for UPDATE operations (true if filter columns are part of replica identity or UPDATE is not published)
- `rf_valid_for_delete`: Boolean indicating whether row filters are valid for DELETE operations (true if filter columns are part of replica identity or DELETE is not published)
- `cols_valid_for_update`: Boolean indicating whether column lists are valid for UPDATE operations (true if columns are part of replica identity or UPDATE is not published)
- `cols_valid_for_delete`: Boolean indicating whether column lists are valid for DELETE operations (true if columns are part of replica identity or DELETE is not published)
## Dependencies
- Functions called/Symbols referenced:
  - [PublicationActions](PublicationActions.md) (embedded structure at line 82)
- Called from (representative examples):
  - [CheckCmdReplicaIdentity](../C/CheckCmdReplicaIdentity.md) (src/backend/executor/execReplication.c:658)
  - [RelationBuildPublicationDesc](../R/RelationBuildPublicationDesc.md) (src/backend/utils/cache/relcache.c:5728)
  - RelationData (src/include/utils/rel.h:168)
  - IndexAttrBitmapKind (src/include/utils/relcache.h:81)

## Notes and Other Information
- This structure is primarily used in the relation cache system to store publication metadata for efficient access
- The validation flags are precomputed to avoid repeated checks during replication operations
- Row filters and column lists are advanced features that allow fine-grained control over which data is replicated
- The validation logic ensures that UPDATE and DELETE operations can be properly identified on the subscriber side using the replica identity
- The structure is part of the RelationData cache entry, making publication information readily available for replication decisions