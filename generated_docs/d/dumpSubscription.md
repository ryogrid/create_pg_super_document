Documentation for dumpSubscription function.

# dumpSubscription

## Location
src/bin/pg_dump/pg_dump.c: 5153 - 5299

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
  - createPQExpBuffer
  - pg_strdup
  - fmtId
  - appendPQExpBuffer
  - appendStringLiteralAH
  - parsePGArray
  - pg_fatal
  - LOGICALREP_TWOPHASE_STATE_DISABLED
  - pg_strcasecmp
  - LOGICALREP_ORIGIN_ANY
  - DUMP_COMPONENT_DEFINITION
  - ArchiveEntry
  - ARCHIVE_OPTS
  - SECTION_POST_DATA
  - DUMP_COMPONENT_COMMENT
  - dumpComment
  - DUMP_COMPONENT_SECLABEL
  - dumpSecLabel
  - destroyPQExpBuffer
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