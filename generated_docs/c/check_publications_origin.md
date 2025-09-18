# check_publications_origin

## Location
src/backend/commands/subscriptioncmds.c: 2032 - 2139

## Overview
Validates and warns when a subscription might copy data with different origins during initial synchronization with copy_data=true and origin=none settings.

## Definition


## Detailed Description
This function performs a critical validation check for logical replication subscriptions to detect potential data origin conflicts. When creating or refreshing a subscription with copy_data=true and origin=none, it queries the publisher to determine if any of the subscribed tables are also being written to by other subscriptions (indicating potential origin conflicts). The function constructs a complex SQL query that examines publication tables, their partition hierarchies, and existing subscription relationships to identify overlapping publications. If conflicts are detected, it logs a warning to alert administrators about potential data origin issues during initial synchronization.

## Parameters / Member Variables
- : Active WAL receiver connection to the publisher database
- : List of publication names being subscribed to
- : Boolean indicating whether initial data copy is requested
- : Origin setting for the subscription (checked for 'none' value)
- : Array of relation OIDs already present on the subscriber
- : Number of relations in the subrel_local_oids array
- : Name of the subscription being created/modified

## Dependencies
- Functions called/Symbols referenced:
  - pg_strcasecmp
  - initStringInfo
  - appendStringInfoString
  - get_publications_str
  - get_namespace_name
  - get_rel_namespace
  - get_rel_name
  - appendStringInfo
  - walrcv_exec
  - pfree
  - ereport
  - MakeSingleTupleTableSlot
  - tuplestore_gettupleslot
  - slot_getattr
  - TextDatumGetCString
  - ExecClearTuple
  - list_append_unique
  - makeString
  - makeStringInfo
  - errdetail_plural
  - ExecDropSingleTupleTableSlot
  - walrcv_clear_result
- Called from (representative examples):
  - CreateSubscription

## Notes and Other Information
- This validation only applies when copy_data=true and origin='none' to prevent silent data corruption issues
- The function excludes already synchronized tables (from subrel_local_oids) since they use WAL-based replication with origin tracking
- Uses complex SQL with partition hierarchy functions (pg_partition_ancestors, pg_partition_tree) to check parent and child table relationships
- Warning messages use plural forms to handle single vs. multiple publication scenarios appropriately
- The check is designed to be conservative - it warns even if tables might be empty, prioritizing safety over precision