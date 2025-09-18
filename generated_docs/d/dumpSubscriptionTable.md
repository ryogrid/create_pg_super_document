Documentation for dumpSubscriptionTable function.

# dumpSubscriptionTable

## Location
src/bin/pg_dump/pg_dump.c: 5084 - 5152

## Overview
Generates SQL commands to restore subscription table relationships during binary upgrades, preserving the exact replication state of subscription-table mappings for PostgreSQL 17 and later.

## Definition
```c
static void dumpSubscriptionTable(Archive *fout, const SubRelInfo *subrinfo)
```

## Detailed Description
This function creates the SQL statements needed to restore subscription table membership during binary upgrades. It calls the `binary_upgrade_add_sub_rel_state()` function to recreate entries in the `pg_subscription_rel` system catalog with the exact same subscription state and LSN position as before the upgrade. The function ensures that subscription table relationships are preserved across major version upgrades, maintaining replication continuity. It skips data-only dumps since this is purely structural information and creates an archive entry in the POST_DATA section to ensure proper restoration order.

## Parameters / Member Variables
- `fout`: Archive handle for writing dump output
- `subrinfo`: SubRelInfo structure containing subscription table relationship details including subscription info, table info, subscription state, and LSN position

## Dependencies
- Functions called/Symbols referenced:
  - DumpOptions
  - SubscriptionInfo
  - [psprintf](../p/psprintf.md)
  - createPQExpBuffer
  - DUMP_COMPONENT_DEFINITION
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - appendStringLiteralAH
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - ARCHIVE_OPTS
  - SECTION_POST_DATA
  - free
  - destroyPQExpBuffer
- Called from (representative examples):
  - Binary upgrade dump process

## Notes and Other Information
- Only used in binary-upgrade mode for PostgreSQL 17 and later versions
- Skips execution during data-only dumps as this is schema/structure information
- Does not create drop statements since subscription table relationships are cleaned up by table drops
- Sets ownership to the subscription owner to ensure proper restoration permissions
- Cannot have comments or security labels as these are not supported for subscription table relationships
- Critical for maintaining replication state continuity across major version upgrades