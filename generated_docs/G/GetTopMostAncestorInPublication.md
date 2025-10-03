# GetTopMostAncestorInPublication

## Location
[src/backend/catalog/pg_publication.c:311-357](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_publication.c#L311-L357)

## Overview
A function that finds the topmost ancestor table in a partition hierarchy that is published in a specified publication, either directly or through schema publication.

## Definition

```c
Oid
GetTopMostAncestorInPublication(Oid puboid, List *ancestors, int *ancestor_level)
```
## Detailed Description
This function traverses a list of ancestor tables (ordered from immediate parent to topmost ancestor) to find the highest-level ancestor that is included in the specified publication. It checks each ancestor both for direct inclusion in the publication's table list and for indirect inclusion through schema-level publication. The function tracks the hierarchical level of the found ancestor, allowing callers to compare results across multiple publications to determine which represents the highest-level publication. This is essential for PostgreSQL's logical replication system to determine the appropriate replication behavior for partitioned tables.

## Parameters / Member Variables
- `puboid`: The OID of the publication to search within
- `*ancestors`: A List of ancestor table OIDs ordered such that the topmost ancestor is at the end
- `*ancestor_level`: A pointer to an integer that will be set to the hierarchical level of the found ancestor (optional, can be NULL)
## Dependencies
- Functions called/Symbols referenced:
  -  (function to get publications containing a relation)
  -  (function to get publications containing a schema)
  -  (function to get the namespace/schema of a relation)
  -  (function to check if an OID is in a list)
  -  (function to free list memory)
  - ,  (List iteration macros)
  -  (constant for invalid OID value)
- Called from (representative examples):
  -  (src/backend/commands/publicationcmds.c:286)
  -  (src/backend/commands/publicationcmds.c:354)
  -  (src/backend/replication/pgoutput/pgoutput.c:2182)
  - Referenced in  (src/include/catalog/pg_publication.h:148)

## Notes and Other Information
This function is crucial for resolving publication inheritance in partitioned table hierarchies. It handles both direct table publication and schema-based publication, ensuring that partitions can inherit publication settings from their ancestors. The ancestor_level parameter enables comparison between multiple publications to determine which provides the most specific or highest-level coverage. The function properly manages memory by freeing temporary lists created during the search process. The ordering requirement for the ancestors list (topmost last) is important for the level calculation to work correctly.

## Simplified Source

```c
Oid
GetTopMostAncestorInPublication(Oid puboid, List *ancestors, int *ancestor_level)
{
    ListCell *lc;
    Oid topmost_relid = InvalidOid;
    int level = 0;

    // Find the "topmost" ancestor that is in this publication
    foreach(lc, ancestors)
    {
        Oid ancestor = lfirst_oid(lc);
        List *apubids = GetRelationPublications(ancestor);
        List *aschemaPubids = NIL;

        level++;

        // Check if ancestor is directly in the publication
        if (list_member_oid(apubids, puboid))
        {
            topmost_relid = ancestor;
            if (ancestor_level)
                *ancestor_level = level;
        }
        else
        {
            // Check if ancestor's schema is in the publication
            aschemaPubids = GetSchemaPublications(get_rel_namespace(ancestor));
            if (list_member_oid(aschemaPubids, puboid))
            {
                topmost_relid = ancestor;
                if (ancestor_level)
                    *ancestor_level = level;
            }
        }

        // Clean up temporary lists
        list_free(apubids);
        list_free(aschemaPubids);
    }

    return topmost_relid;
}
```