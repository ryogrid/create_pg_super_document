# SubRelInfo

## Location
src/bin/pg_dump/pg_dump.h: 708 - 798

## Overview
SubRelInfo is a structure used by pg_dump to represent the relationship between PostgreSQL logical replication subscriptions and specific tables, storing metadata necessary for dumping and restoring subscription table membership.

## Definition


## Detailed Description
SubRelInfo represents individual subscription-table relationships in PostgreSQL's logical replication system. Each instance captures the state of a single table within a specific subscription, including replication state and synchronization position. This structure is primarily used during binary upgrade operations in PostgreSQL 17 and later to preserve subscription table membership and state information across version upgrades. It extends the DumpableObject pattern to integrate with pg_dump's dependency tracking and selective dumping mechanisms.

## Parameters / Member Variables
- : Base DumpableObject containing object identification, dump control flags, and dependency information
- : Pointer to the SubscriptionInfo structure representing the parent subscription
- : Pointer to the TableInfo structure representing the subscribed table
- : Character representing the synchronization state of this table within the subscription (e.g., 'i' for initialize, 'r' for ready, 's' for synchronized)
- : String representation of the Log Sequence Number (LSN) position for this table's replication state, or NULL if not applicable

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (base structure)
  - SubscriptionInfo (parent subscription)
  - TableInfo (subscribed table)
- Called from (representative examples):
  - getSubscriptionTables
  - dumpSubscriptionTable
  - fmtQualifiedDumpable

## Notes and Other Information
- Only used in binary-upgrade mode for PostgreSQL 17 and later versions
- Part of PostgreSQL's logical replication infrastructure for streaming changes between databases
- The srsubstate field corresponds to the substate column in pg_subscription_rel system catalog
- LSN tracking allows for proper resumption of replication after restoration
- Each SubRelInfo represents a single row from the pg_subscription_rel system catalog
- Used to maintain subscription table membership during database upgrades