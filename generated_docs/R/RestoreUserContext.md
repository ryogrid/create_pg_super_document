# RestoreUserContext

## Location
src/backend/utils/init/usercontext.c: 87 - 92

## Overview
Restores the original user ID and security context after a temporary switch, rolling back any configuration changes made during the switched context.

## Definition
```c
void RestoreUserContext(UserContext *context)
```

## Detailed Description
This function serves as the cleanup counterpart to SwitchToUntrustedUser(), restoring the session to its original user context. It performs two key operations:
1. Rolls back any GUC (Grand Unified Configuration) changes that were made while operating under the switched user context
2. Restores the original user ID and security context

The function checks if a GUC nest level was created during the user switch (indicated by save_nestlevel != -1). If so, it calls AtEOXact_GUC() to roll back any configuration parameter changes that might have been made by code running as the switched user, ensuring that potentially malicious configuration changes don't persist.

## Parameters / Member Variables
- `context`: Pointer to UserContext structure containing the saved original user context (user ID, security context, and GUC nest level)

## Dependencies
- Functions called/Symbols referenced:
  - AtEOXact_GUC
  - SetUserIdAndSecContext
- Called from (representative examples):
  - ExecuteTruncateGuts (tablecmds.c:2065, 2274)
  - LogicalRepSyncTableStart (tablesync.c:1549)
  - apply_handle_insert (worker.c:2451)
  - apply_handle_update (worker.c:2630)
  - apply_handle_delete (worker.c:2791)

## Notes and Other Information
- Must be called after every SwitchToUntrustedUser() call to prevent context leaks
- The save_nestlevel field in UserContext determines whether GUC rollback is needed (-1 means no rollback required)
- Typically used in finally blocks or cleanup code paths to ensure proper context restoration even in error cases
- Critical for maintaining security boundaries and preventing configuration parameter pollution between user contexts
- Used extensively in logical replication operations where temporary privilege changes are common