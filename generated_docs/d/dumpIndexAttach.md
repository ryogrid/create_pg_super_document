# dumpIndexAttach

## Location
[src/bin/pg_dump/pg_dump.c:17117-17159](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L17117-L17159)

## Overview
Writes out a partitioned-index attachment clause that attaches a partition index to its parent partitioned index using ALTER INDEX ATTACH PARTITION syntax.

## Definition

```c
static void
dumpIndexAttach(Archive *fout, const IndexAttachInfo *attachinfo)
```
## Detailed Description
The  function generates SQL commands for attaching partition indexes to their parent partitioned indexes. This is part of PostgreSQL's partitioned index infrastructure where individual partition indexes need to be explicitly attached to the main partitioned index.

The function creates an  statement that establishes the relationship between a partitioned index and its partition-specific index. This attachment is essential for the partitioned index to function correctly across all partitions.

Key aspects:
1. **No Drop Statement**: Unlike regular objects, index attachments don't need explicit DROP statements since detachment happens automatically when dropping either the partition table or the partitioned index
2. **No DETACH Command**: PostgreSQL doesn't provide , so there's no way to reverse this operation explicitly
3. **Ownership Handling**: Uses the parent index's table owner to ensure the command runs with correct privileges during restore

## Parameters / Member Variables
- `*fout`: Archive pointer containing dump options and output context
- `*attachinfo`: IndexAttachInfo structure containing:
## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - fmtQualifiedDumpable
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [destroyPQExpBuffer](destroyPQExpBuffer.md)
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md)

## Notes and Other Information
- Skips processing in data-only dump mode
- Only processes if the partition index has DUMP_COMPONENT_DEFINITION flag set
- Uses SECTION_POST_DATA section to ensure proper restoration order
- The attachment object inherits the owner from the parent index's table for security purposes
- Part of PostgreSQL's declarative partitioning feature introduced for better partitioned index management