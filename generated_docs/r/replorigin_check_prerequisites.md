# replorigin_check_prerequisites

## Location
src/backend/replication/logical/origin.c: 185 - 203

## Overview
A static helper function that validates prerequisites for replication origin operations, ensuring that replication slots are configured when needed and that operations are not performed during recovery when not allowed.

## Definition

```c
static void
replorigin_check_prerequisites(bool check_slots, bool recoveryOK)
```
## Detailed Description
This function performs essential prerequisite checks before allowing replication origin operations to proceed. It validates two critical conditions: first, it ensures that replication slots are properly configured (max_replication_slots > 0) when slot checking is required, and second, it prevents certain operations during recovery unless explicitly permitted. The function acts as a gatekeeper, throwing appropriate errors when prerequisites are not met, thereby maintaining the integrity and safety of replication origin operations.

## Parameters / Member Variables
- : Boolean flag indicating whether to verify that replication slots are configured (max_replication_slots > 0)
- : Boolean flag indicating whether the operation is permitted during recovery mode

## Dependencies
- Functions called/Symbols referenced:
  - RecoveryInProgress
  - ereport
  - errcode
  - errmsg
- Called from (representative examples):
  - pg_replication_origin_create
  - pg_replication_origin_drop
  - pg_replication_origin_oid
  - pg_replication_origin_session_setup
  - pg_replication_origin_session_reset
  - pg_replication_origin_session_is_setup
  - pg_replication_origin_session_progress
  - pg_replication_origin_xact_setup
  - pg_replication_origin_xact_reset
  - pg_replication_origin_advance
  - pg_replication_origin_progress

## Notes and Other Information
- This is a static function, meaning it's only accessible within the origin.c file
- Throws ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE when replication slots are not configured but required
- Throws ERRCODE_READ_ONLY_SQL_TRANSACTION when attempting restricted operations during recovery
- Serves as a centralized validation point for all SQL-callable replication origin functions
- The function is called by virtually all public replication origin API functions to ensure consistent prerequisite checking