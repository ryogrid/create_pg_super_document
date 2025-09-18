# CheckPubRelationColumnList

## Location
src/backend/commands/publicationcmds.c: 677 - 727

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
  - PublicationRelInfo
  - get_namespace_name
  - RelationGetNamespace
  - RelationGetRelationName
- Called from:
  - CreatePublication
  - AlterPublicationTables

## Notes and Other Information
- This is a static function used internally within publicationcmds.c
- The function only processes relations that have non-empty column lists (pri->columns != NIL)
- Error messages include fully qualified relation names (schema.table) for clarity
- The restriction against column lists with schema publication is designed to avoid complex interactions and potential conflicts
- For partitioned tables, the restriction exists because individual partitions would use their own column lists when publish_via_partition_root is false
- The function provides detailed explanations in error messages about why specific restrictions exist