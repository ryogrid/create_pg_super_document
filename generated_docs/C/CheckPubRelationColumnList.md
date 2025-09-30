# CheckPubRelationColumnList

## Location
[src/backend/commands/publicationcmds.c:677-727](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/publicationcmds.c#L677-L727)

## Overview
Validates column list specifications for publication relations, enforcing restrictions based on schema publication settings and partition root publishing configuration.

## Definition
```c
static void CheckPubRelationColumnList(char *pubname, List *tables, bool publish_schema, bool pubviaroot)
```

## Detailed Description
This function enforces two critical restrictions on publication column lists to ensure proper logical replication behavior:

1. **Schema Publication Restriction**: Column lists are forbidden when the publication contains any "TABLES IN SCHEMA" elements. This prevents conflicts between explicit column specifications and schema-based publication rules.

2. **Partitioned Table Restriction**: Column lists are forbidden for partitioned tables when publish_via_partition_root is false. In this configuration, individual partition column lists would be used, making column lists on the partitioned table meaningless and potentially confusing.

The function iterates through all provided tables, checking each one with a column list against these restrictions and providing detailed error messages when violations are found.

## Parameters / Member Variables
- `pubname`: Name of the publication for error reporting
- `tables`: List of PublicationRelInfo structures representing tables to be added
- `publish_schema`: Boolean indicating if publication contains TABLES IN SCHEMA elements
- `pubviaroot`: Boolean indicating the value of publish_via_partition_root setting

## Dependencies
- Functions called/Symbols referenced:
  - [PublicationRelInfo](../P/PublicationRelInfo.md)
  - [get_namespace_name](../g/get_namespace_name.md)
  - RelationGetNamespace
  - RelationGetRelationName
- Called from:
  - [CreatePublication](CreatePublication.md)
  - [AlterPublicationTables](../A/AlterPublicationTables.md)

## Notes and Other Information
- This is a static function used internally within publicationcmds.c
- The function only processes relations that have non-empty column lists (pri->columns != NIL)
- Error messages include fully qualified relation names (schema.table) for clarity
- The restriction against column lists with schema publication is designed to avoid complex interactions and potential conflicts
- For partitioned tables, the restriction exists because individual partitions would use their own column lists when publish_via_partition_root is false
- The function provides detailed explanations in error messages about why specific restrictions exist

## Simplified Source

```c
static void CheckPubRelationColumnList(char *pubname, List *tables,
                                      bool publish_schema, bool pubviaroot)
{
    ListCell *lc;

    foreach(lc, tables) {
        PublicationRelInfo *pri = (PublicationRelInfo *) lfirst(lc);

        if (pri->columns == NIL) {
            continue;
        }

        // Disallow column lists when any schema is in the publication
        if (publish_schema) {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("cannot use column list for relation \"%s.%s\" in publication \"%s\"",
                            get_namespace_name(RelationGetNamespace(pri->relation)),
                            RelationGetRelationName(pri->relation), pubname),
                     errdetail("Column lists cannot be specified in publications containing FOR TABLES IN SCHEMA elements.")));
        }

        // Disallow column lists on partitioned tables when not publishing via root
        if (!pubviaroot && pri->relation->rd_rel->relkind == RELKIND_PARTITIONED_TABLE) {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("cannot use column list for relation \"%s.%s\" in publication \"%s\"",
                            get_namespace_name(RelationGetNamespace(pri->relation)),
                            RelationGetRelationName(pri->relation), pubname),
                     errdetail("Column lists cannot be specified for partitioned tables when %s is false.",
                               "publish_via_partition_root")));
        }
    }
}
```