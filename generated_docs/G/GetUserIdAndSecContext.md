# GetUserIdAndSecContext

## Location
[src/backend/utils/init/miscinit.c:658-664](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L658-L664)

## Overview
Retrieves both the current effective user ID and the security restriction context flags, providing a complete snapshot of the current security state for transaction and operation management.

## Definition

```c
void
GetUserIdAndSecContext(Oid *userid, int *sec_context)
```
## Detailed Description
GetUserIdAndSecContext is a critical function in PostgreSQL's security and transaction management system that atomically retrieves both the current effective user ID (CurrentUserId) and the security restriction context flags. Unlike other user ID functions, this function is designed to never fail or assert, making it safe for use during transaction startup, abort, and other critical error handling paths.

The function serves three main purposes:
1. **Transaction State Management**: Used by StartTransaction, PushTransaction, and AbortTransaction to save/restore security context
2. **Security Context Preservation**: Captures the complete security state before switching to different user contexts
3. **Error Handling**: Provides a fail-safe way to retrieve security state even when CurrentUserId might be invalid

The SecurityRestrictionContext contains flags that indicate various security restrictions currently in effect, including:
- SECURITY_LOCAL_USERID_CHANGE: Indicates temporary user ID changes are in progress
- SECURITY_RESTRICTED_OPERATION: Used during operations like autovacuum and REINDEX that require heightened security
- SECURITY_NOFORCE_RLS: Disables forced row-level security for referential integrity checks

## Parameters / Member Variables
- : Pointer to receive the current effective user ID (CurrentUserId). May be InvalidOid in some contexts.
- : Pointer to receive the current SecurityRestrictionContext flags as a bitmask.

## Dependencies
- Functions called/Symbols referenced:
  - CurrentUserId (global static variable access)
  - SecurityRestrictionContext (global static variable access)
- Called from (representative examples):
  - [StartTransaction](../S/StartTransaction.md), PushTransaction (transaction management)
  - [brin_summarize_range](../b/brin_summarize_range.md), do_analyze_rel, cluster_rel (maintenance operations)
  - [DefineIndex](../D/DefineIndex.md), reindex_index (index operations)
  - [fmgr_security_definer](../f/fmgr_security_definer.md) (security definer functions)
  - [SwitchToUntrustedUser](../S/SwitchToUntrustedUser.md) (user context switching)

## Notes and Other Information
- This function never throws errors or assertions, making it safe for error recovery contexts
- Unlike GetUserId, it does not assert that CurrentUserId is valid
- Designed specifically for transaction boundaries and security context switching
- The function provides atomic access to both security-related global variables
- Critical for maintaining security state consistency across complex operations like parallel queries, index maintenance, and transaction processing
- Used extensively in PostgreSQL's security definer function execution and restricted operation handling