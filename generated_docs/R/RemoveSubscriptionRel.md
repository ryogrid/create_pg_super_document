# RemoveSubscriptionRel

## Location
src/backend/catalog/pg_subscription.c: 416 - 490

## Overview
Removes subscription relation mapping entries from the pg_subscription_rel system catalog, supporting flexible deletion by subscription, relation, or both with validation for in-progress synchronization.

## Definition


## Detailed Description
This function removes one or more entries from the pg_subscription_rel catalog based on the provided subscription and/or relation OIDs. It supports three deletion modes: removing all relations for a specific subscription (when relid is invalid), removing a specific relation from all subscriptions (when subid is invalid), or removing a specific subscription-relation pair (when both are valid).

The function includes important safety checks to prevent removal of relation mappings when table synchronization is in progress, unless the entire subscription is being updated. This prevents orphaned tablesync slots or origins from remaining in the system. It uses a catalog scan with appropriate scan keys to locate matching entries and deletes them using CatalogTupleDelete.

## Parameters / Member Variables
- : The OID of the subscription (can be InvalidOid to affect all subscriptions)
- : The OID of the relation (can be InvalidOid to affect all relations)

## Dependencies
- Functions called/Symbols referenced:
  - TableScanDesc
  - table_beginscan_catalog
  - heap_getnext
  - ForwardScanDirection
  - Form_pg_subscription_rel
  - SUBREL_STATE_READY
  - get_subscription_name
  - get_rel_name
  - CatalogTupleDelete
  - table_endscan
- Called from (representative examples):
  - heap_drop_with_catalog
  - DropSubscription

## Notes and Other Information
- Supports flexible deletion patterns: specific subscription-relation pairs, all relations for a subscription, or a relation from all subscriptions
- Implements safety validation to prevent removal during active table synchronization (unless removing entire subscription)
- Provides detailed error messages with hints for resolving synchronization conflicts
- Uses catalog scanning to efficiently locate and remove matching entries
- Critical for maintaining consistency when dropping tables or subscriptions in logical replication
- Located in src/backend/catalog/pg_subscription.c:416-490