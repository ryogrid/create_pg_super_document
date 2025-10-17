Documentation for dumpSubscription function.

# dumpSubscription

## Location
[src/bin/pg_dump/pg_dump.c:5153-5299](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L5153-L5299)

## Overview
Generates CREATE SUBSCRIPTION SQL statements and related commands to restore logical replication subscriptions, including all subscription parameters and binary upgrade specific state preservation.

## Definition
```c
static void dumpSubscription(Archive *fout, const SubscriptionInfo *subinfo)
```

## Detailed Description
This function creates the complete SQL DDL needed to recreate a subscription during restore. It constructs a CREATE SUBSCRIPTION statement with all the subscription parameters including connection info, publications, and various replication options (binary format, streaming, two-phase commit, etc.). The function handles version-specific features and generates appropriate WITH clauses based on the subscription configuration. For binary upgrades in PostgreSQL 17+, it also includes additional commands to preserve replication origin LSNs and enable the subscription to continue replication after upgrade. The function also generates DROP SUBSCRIPTION statements and handles comments and security labels.

## Parameters / Member Variables
- `fout`: Archive handle for writing dump output
- `subinfo`: SubscriptionInfo structure containing all subscription properties including name, owner, connection info, publications, and various boolean settings

## Dependencies
- Functions called/Symbols referenced:
  - DumpOptions
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [pg_strdup](../p/pg_strdup.md)
  - [fmtId](../f/fmtId.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - appendStringLiteralAH
  - [parsePGArray](../p/parsePGArray.md)
  - [pg_fatal](../p/pg_fatal.md)
  - LOGICALREP_TWOPHASE_STATE_DISABLED
  - [pg_strcasecmp](../p/pg_strcasecmp.md)
  - LOGICALREP_ORIGIN_ANY
  - DUMP_COMPONENT_DEFINITION
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - ARCHIVE_OPTS
  - SECTION_POST_DATA
  - DUMP_COMPONENT_COMMENT
  - [dumpComment](dumpComment.md)
  - DUMP_COMPONENT_SECLABEL
  - [dumpSecLabel](dumpSecLabel.md)
  - [destroyPQExpBuffer](destroyPQExpBuffer.md)
  - free
- Called from (representative examples):
  - Main dump process for subscriptions

## Notes and Other Information
- Skips execution during data-only dumps as subscriptions are schema objects
- Creates subscriptions with connect=false to prevent immediate connection during restore
- Handles all subscription parameters including binary format, streaming modes, two-phase commit, error handling, authentication, and failover settings
- For binary upgrades, preserves replication origin remote LSN and enabled state to maintain replication continuity
- Supports comments and security labels on subscription objects
- Parses publication arrays to handle multiple publications per subscription
- Uses proper SQL identifier quoting for subscription and publication names

## Simplified Source

```c
static void dumpSubscription(Archive *fout, const SubscriptionInfo *subinfo) {
    DumpOptions *dopt = fout->dopt;
    PQExpBuffer delq;
    PQExpBuffer query;
    PQExpBuffer publications;
    char *qsubname;
    char **pubnames = NULL;
    int npubnames = 0;

    // Skip in data-only dumps
    if (dopt->dataOnly)
        return;

    delq = createPQExpBuffer();
    query = createPQExpBuffer();

    qsubname = pg_strdup(fmtId(subinfo->dobj.name));

    // Generate DROP SUBSCRIPTION statement
    appendPQExpBuffer(delq, "DROP SUBSCRIPTION %s;\n", qsubname);

    // Start CREATE SUBSCRIPTION statement
    appendPQExpBuffer(query, "CREATE SUBSCRIPTION %s CONNECTION ", qsubname);
    appendStringLiteralAH(query, subinfo->subconninfo, fout);

    // Parse and format publications list
    parsePGArray(subinfo->subpublications, &pubnames, &npubnames);
    publications = createPQExpBuffer();
    for (int i = 0; i < npubnames; i++) {
        if (i > 0)
            appendPQExpBufferStr(publications, ", ");
        appendPQExpBufferStr(publications, fmtId(pubnames[i]));
    }

    // Add PUBLICATION clause with connect=false and slot_name
    appendPQExpBuffer(query, " PUBLICATION %s WITH (connect = false, slot_name = ",
                      publications->data);
    if (subinfo->subslotname)
        appendStringLiteralAH(query, subinfo->subslotname, fout);
    else
        appendPQExpBufferStr(query, "NONE");

    // Add subscription options based on configuration
    if (strcmp(subinfo->subbinary, "t") == 0)
        appendPQExpBufferStr(query, ", binary = true");

    if (strcmp(subinfo->substream, "t") == 0)
        appendPQExpBufferStr(query, ", streaming = on");
    else if (strcmp(subinfo->substream, "p") == 0)
        appendPQExpBufferStr(query, ", streaming = parallel");

    char two_phase_disabled[] = {LOGICALREP_TWOPHASE_STATE_DISABLED, '\0'};
    if (strcmp(subinfo->subtwophasestate, two_phase_disabled) != 0)
        appendPQExpBufferStr(query, ", two_phase = on");

    if (strcmp(subinfo->subdisableonerr, "t") == 0)
        appendPQExpBufferStr(query, ", disable_on_error = true");

    if (strcmp(subinfo->subpasswordrequired, "t") != 0)
        appendPQExpBuffer(query, ", password_required = false");

    if (strcmp(subinfo->subrunasowner, "t") == 0)
        appendPQExpBufferStr(query, ", run_as_owner = true");

    if (strcmp(subinfo->subfailover, "t") == 0)
        appendPQExpBufferStr(query, ", failover = true");

    if (strcmp(subinfo->subsynccommit, "off") != 0)
        appendPQExpBuffer(query, ", synchronous_commit = %s", fmtId(subinfo->subsynccommit));

    if (pg_strcasecmp(subinfo->suborigin, LOGICALREP_ORIGIN_ANY) != 0)
        appendPQExpBuffer(query, ", origin = %s", subinfo->suborigin);

    appendPQExpBufferStr(query, ");\n");

    // Handle binary upgrade specific operations
    if (dopt->binary_upgrade && fout->remoteVersion >= 170000) {
        // Preserve replication origin remote LSN
        if (subinfo->suboriginremotelsn) {
            appendPQExpBufferStr(query,
                                 "\n-- For binary upgrade, must preserve the remote_lsn for the subscriber's replication origin.\n");
            appendPQExpBufferStr(query,
                                 "SELECT pg_catalog.binary_upgrade_replorigin_advance(");
            appendStringLiteralAH(query, subinfo->dobj.name, fout);
            appendPQExpBuffer(query, ", '%s');\n", subinfo->suboriginremotelsn);
        }

        // Enable subscription if it was enabled before
        if (strcmp(subinfo->subenabled, "t") == 0) {
            appendPQExpBufferStr(query,
                                 "\n-- For binary upgrade, must preserve the subscriber's running state.\n");
            appendPQExpBuffer(query, "ALTER SUBSCRIPTION %s ENABLE;\n", qsubname);
        }
    }

    // Create archive entry
    if (subinfo->dobj.dump & DUMP_COMPONENT_DEFINITION) {
        ArchiveEntry(fout, subinfo->dobj.catId, subinfo->dobj.dumpId,
                     ARCHIVE_OPTS(.tag = subinfo->dobj.name,
                                  .owner = subinfo->rolname,
                                  .description = "SUBSCRIPTION",
                                  .section = SECTION_POST_DATA,
                                  .createStmt = query->data,
                                  .dropStmt = delq->data));
    }

    // Handle comments and security labels
    if (subinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
        dumpComment(fout, "SUBSCRIPTION", qsubname, NULL, subinfo->rolname,
                    subinfo->dobj.catId, 0, subinfo->dobj.dumpId);

    if (subinfo->dobj.dump & DUMP_COMPONENT_SECLABEL)
        dumpSecLabel(fout, "SUBSCRIPTION", qsubname, NULL, subinfo->rolname,
                     subinfo->dobj.catId, 0, subinfo->dobj.dumpId);

    // Cleanup
    destroyPQExpBuffer(publications);
    free(pubnames);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(query);
    free(qsubname);
}
```