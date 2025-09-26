# pgstat_delete_pending_entry

## Location
src/backend/utils/activity/pgstat.c: 1158 - 1181

## Overview
Deletes a pending statistics entry and cleans up associated resources, calling kind-specific deletion callbacks if available and removing the entry from the pending list.

## Definition
void pgstat_delete_pending_entry(PgStat_EntryRef *entry_ref)

## Detailed Description
This function handles the complete deletion of a pending statistics entry from the PostgreSQL statistics collection system. It performs several cleanup operations: first, it calls any kind-specific deletion callback if one exists for the statistics type, then frees the pending data memory, sets the pending pointer to NULL, and finally removes the entry from the pending doubly-linked list. The function includes safety assertions to ensure the entry has pending data and that it represents a variable-amount statistics type (not fixed_amount), as fixed-amount statistics require explicit handling elsewhere.

## Parameters / Member Variables
- `entry_ref`: A reference to the statistics entry containing the pending data to be deleted

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_get_kind_info
  - pfree
  - dlist_delete
  - PgStat_Kind
  - PgStat_KindInfo
  - PgStat_EntryRef
- Called from (representative examples):
  - pgstat_flush_pending_entries (src/backend/utils/activity/pgstat.c:1225)
  - pgstat_release_entry_ref (src/backend/utils/activity/pgstat_shmem.c:556)

## Notes and Other Information
- Requires that pending_data is not NULL (enforced by assertion)
- Only handles variable-amount statistics (fixed_amount stats must be handled explicitly elsewhere)
- Calls kind-specific deletion callbacks when available to allow for custom cleanup logic
- Removes the entry from the pending doubly-linked list to maintain list integrity
- Part of PostgreSQL's statistics memory management and cleanup infrastructure