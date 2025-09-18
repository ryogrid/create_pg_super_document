# ResolveRecoveryConflictWithTablespace

## Location
[src/backend/storage/ipc/standby.c:538-567](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/standby.c#L538-L567)

## Overview
This function resolves recovery conflicts with backends that are currently using temporary files in a tablespace that is being dropped during WAL replay.

## Definition
```c
void ResolveRecoveryConflictWithTablespace(Oid tsid)
```

## Detailed Description
ResolveRecoveryConflictWithTablespace handles a specific type of recovery conflict that occurs when a tablespace needs to be dropped during standby recovery, but standby backends are currently using that tablespace for temporary files. This function implements an aggressive conflict resolution strategy that immediately cancels all queries to ensure the tablespace can be safely removed.

The function adopts a "nuclear" approach (as humorously noted in the comment "Nuke the entire site from orbit, it's the only way to be sure") by requesting cancellation of all backends rather than trying to selectively identify which ones are using the specific tablespace. This design choice prioritizes correctness and simplicity over optimization, ensuring that no temporary files remain that could prevent tablespace removal.

The function operates differently from other conflict resolution mechanisms by using InvalidTransactionId and InvalidOid as parameters to GetConflictingVirtualXIDs, effectively targeting all active virtual transactions rather than those conflicting with specific transaction IDs or databases. This broad approach ensures complete cleanup but may be more disruptive than strictly necessary.

## Parameters / Member Variables
- `tsid`: Oid of the tablespace that is being dropped and needs conflict resolution

## Dependencies
- Functions called/Symbols referenced:
  - GetConflictingVirtualXIDs
  - [ResolveRecoveryConflictWithVirtualXIDs](ResolveRecoveryConflictWithVirtualXIDs.md)
  - PROCSIG_RECOVERY_CONFLICT_TABLESPACE
  - WAIT_EVENT_RECOVERY_CONFLICT_TABLESPACE
  - InvalidTransactionId
  - InvalidOid
- Called from (representative examples):
  - [tblspc_redo](../t/tblspc_redo.md)

## Notes and Other Information
- The function uses an intentionally broad conflict resolution approach, canceling all backends rather than trying to identify specific ones using the tablespace
- Comments indicate a possible future optimization: examining temporary filenames in the tablespace directory to identify specific PIDs and convert them to VirtualXIDs for more targeted cancellation
- The function does not wait for transaction commits because tablespace dropping is a non-transactional operation
- The aggressive approach ensures no temporary files remain that could interfere with tablespace removal
- This function is specifically called during tablespace redo operations when replaying DROP TABLESPACE commands on standby servers
- The tsid parameter is currently not directly used in the conflict resolution logic, but is passed for potential future enhancements
- Recovery conflicts of this type should be relatively rare, occurring only when standby queries happen to be using temporary files in a tablespace being dropped