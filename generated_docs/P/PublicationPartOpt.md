# PublicationPartOpt

## Location
[src/include/catalog/pg_publication.h:134-161](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/catalog/pg_publication.h#L134-L161)

## Overview
PublicationPartOpt is an enumeration that specifies partition handling options for PostgreSQL logical replication publications, controlling which partitions of partitioned tables are included when retrieving publication relationships.

## Definition


## Detailed Description
PublicationPartOpt is a critical enumeration in PostgreSQL's logical replication system that controls how partitioned tables are handled when retrieving relations associated with a publication. This enum provides three distinct strategies for partition inclusion, allowing fine-grained control over which parts of a partitioned table hierarchy are considered for replication. It is primarily used as a parameter to various publication-related functions to specify the caller's expectations about partition visibility and inclusion in replication streams.

The enum is designed to work with PostgreSQL's partitioned table system, where tables can be organized in hierarchical partition trees. Different replication scenarios may require different approaches to handling these partitions, and this enumeration provides the necessary flexibility.

## Parameters / Member Variables
- : Only include the table explicitly mentioned in the publication (root partitioned table only)
- : Only include leaf partitions in the partition tree (actual data-containing partitions)
- : Include all partitions in the partition tree (both root and all descendant partitions)

## Dependencies
- Functions called/Symbols referenced:
  - None (enumeration definition)
- Called from (representative examples):
  - [GetPublicationRelations](../G/GetPublicationRelations.md) (in pg_publication.c:716)
  - [GetSchemaPublicationRelations](../G/GetSchemaPublicationRelations.md) (in pg_publication.c:925)
  - [GetAllSchemaPublicationRelations](../G/GetAllSchemaPublicationRelations.md) (in pg_publication.c:982)
  - [GetPubPartitionOptionRelations](../G/GetPubPartitionOptionRelations.md) (in pg_publication.c:267)

## Notes and Other Information
- Used primarily in logical replication context for publication management
- Affects how partitioned tables are handled during replication setup and maintenance
- ROOT option is useful when only the parent table metadata is needed
- LEAF option is commonly used for actual data replication scenarios
- ALL option provides comprehensive coverage of the entire partition hierarchy
- Critical for determining replication behavior in complex partitioned table environments