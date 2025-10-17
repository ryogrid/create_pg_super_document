Documentation for dumpSubscriptionTable function.

# dumpSubscriptionTable

## Location
[src/bin/pg_dump/pg_dump.c:5084-5152](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L5084-L5152)

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
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - DUMP_COMPONENT_DEFINITION
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - appendStringLiteralAH
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - ARCHIVE_OPTS
  - SECTION_POST_DATA
  - free
  - [destroyPQExpBuffer](destroyPQExpBuffer.md)
- Called from (representative examples):
  - Binary upgrade dump process

## Notes and Other Information
- Only used in binary-upgrade mode for PostgreSQL 17 and later versions
- Skips execution during data-only dumps as this is schema/structure information
- Does not create drop statements since subscription table relationships are cleaned up by table drops
- Sets ownership to the subscription owner to ensure proper restoration permissions
- Cannot have comments or security labels as these are not supported for subscription table relationships
- Critical for maintaining replication state continuity across major version upgrades

## Simplified Source

```c
static void dumpSubscriptionTable(Archive *fout, const SubRelInfo *subrinfo) {
    DumpOptions *dopt = fout->dopt;
    SubscriptionInfo *subinfo = subrinfo->subinfo;
    PQExpBuffer query;
    char *tag;

    // Skip in data-only dumps
    if (dopt->dataOnly)
        return;

    Assert(fout->dopt->binary_upgrade && fout->remoteVersion >= 170000);

    // Create descriptive tag for the archive entry
    tag = psprintf("%s %s", subinfo->dobj.name, subrinfo->dobj.name);

    query = createPQExpBuffer();

    if (subinfo->dobj.dump & DUMP_COMPONENT_DEFINITION) {
        // Generate binary upgrade function call to restore subscription relation state
        appendPQExpBufferStr(query,
                             "\n-- For binary upgrade, must preserve the subscriber table.\n");
        appendPQExpBufferStr(query,
                             "SELECT pg_catalog.binary_upgrade_add_sub_rel_state(");
        appendStringLiteralAH(query, subrinfo->dobj.name, fout);
        appendPQExpBuffer(query,
                          ", %u, '%c'",
                          subrinfo->tblinfo->dobj.catId.oid,
                          subrinfo->srsubstate);

        // Add LSN if present
        if (subrinfo->srsublsn && subrinfo->srsublsn[0] != '\0')
            appendPQExpBuffer(query, ", '%s'", subrinfo->srsublsn);
        else
            appendPQExpBuffer(query, ", NULL");

        appendPQExpBufferStr(query, ");\n");
    }

    // Create archive entry if definition should be dumped
    if (subrinfo->dobj.dump & DUMP_COMPONENT_DEFINITION) {
        ArchiveEntry(fout, subrinfo->dobj.catId, subrinfo->dobj.dumpId,
                     ARCHIVE_OPTS(.tag = tag,
                                  .namespace = subrinfo->tblinfo->dobj.namespace->dobj.name,
                                  .owner = subinfo->rolname,
                                  .description = "SUBSCRIPTION TABLE",
                                  .section = SECTION_POST_DATA,
                                  .createStmt = query->data));
    }

    // Cleanup
    free(tag);
    destroyPQExpBuffer(query);
}
```