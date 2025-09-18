# dumpIndexAttach

## Location
src/bin/pg_dump/pg_dump.c: 17117 - 17159

## Overview
Writes out a partitioned-index attachment clause that attaches a partition index to its parent partitioned index using ALTER INDEX ATTACH PARTITION syntax.

## Definition


## Detailed Description
The  function generates SQL commands for attaching partition indexes to their parent partitioned indexes. This is part of PostgreSQL's partitioned index infrastructure where individual partition indexes need to be explicitly attached to the main partitioned index.

The function creates an  statement that establishes the relationship between a partitioned index and its partition-specific index. This attachment is essential for the partitioned index to function correctly across all partitions.

Key aspects:
1. **No Drop Statement**: Unlike regular objects, index attachments don't need explicit DROP statements since detachment happens automatically when dropping either the partition table or the partitioned index
2. **No DETACH Command**: PostgreSQL doesn't provide , so there's no way to reverse this operation explicitly
3. **Ownership Handling**: Uses the parent index's table owner to ensure the command runs with correct privileges during restore

## Parameters / Member Variables
- : Archive pointer containing dump options and output context
- : IndexAttachInfo structure containing:
  - Parent partitioned index information
  - Partition index information
  - Attachment metadata and dump object details

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer
  - fmtQualifiedDumpable
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - destroyPQExpBuffer
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md)

## Notes and Other Information
- Skips processing in data-only dump mode
- Only processes if the partition index has DUMP_COMPONENT_DEFINITION flag set
- Uses SECTION_POST_DATA section to ensure proper restoration order
- The attachment object inherits the owner from the parent index's table for security purposes
- Part of PostgreSQL's declarative partitioning feature introduced for better partitioned index management