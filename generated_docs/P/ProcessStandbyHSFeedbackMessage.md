# ProcessStandbyHSFeedbackMessage

## Location
src/backend/replication/walsender.c: 2591 - 2714

## Overview
Processes hot standby feedback messages from standby servers that communicate their transaction visibility horizon, preventing premature tuple removal on the primary server that would cause recovery conflicts.

## Definition


## Detailed Description
This function handles hot standby feedback messages that allow standby servers to inform the primary about their current transaction visibility requirements. The feedback contains xmin values representing the oldest transactions still visible to queries running on the standby, enabling the primary to hold back VACUUM operations that would otherwise create recovery conflicts.

The function parses the incoming message to extract both regular and catalog xmin values along with their epochs, validates these transaction IDs for temporal reasonableness, and updates either the replication slot or the WalSender process's xmin accordingly. When using replication slots, both regular and catalog xmin values can be tracked separately; without slots, only the more restrictive (older) of the two is preserved.

The design includes safeguards against invalid feedback values and race conditions, with detailed comments explaining the careful balance between performance and correctness in xmin management. The function also handles the case where hot standby feedback is disabled by clearing xmin constraints.

## Parameters / Member Variables
This function takes no parameters but processes data from the global  buffer containing:
- : Timestamp when the feedback was sent by the standby
- : Oldest transaction ID visible to regular queries on standby
- : Epoch (high bits) for the regular xmin
- : Oldest transaction ID visible to catalog queries on standby  
- : Epoch (high bits) for the catalog xmin

## Dependencies
- Functions called/Symbols referenced:
  - [pq_getmsgint64](../p/pq_getmsgint64.md), pq_getmsgint (message parsing)
  - TransactionIdIsNormal, TransactionIdInRecentPast (validation)
  - [PhysicalReplicationSlotNewXmin](PhysicalReplicationSlotNewXmin.md) (slot-based xmin updates)
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md) (transaction ID comparison)
  - [timestamptz_to_str](../t/timestamptz_to_str.md) (debug logging)
- Called from (representative examples):
  - [ProcessStandbyMessage](ProcessStandbyMessage.md)

## Notes and Other Information
- Critical for preventing recovery conflicts between primary VACUUM and standby queries
- Handles both replication slot-based and process-based xmin reservation mechanisms
- Includes extensive validation to prevent processing of invalid or malicious feedback
- Uses spinlocks for thread-safe updates to shared WalSender state
- Contains detailed race condition analysis and mitigation strategies
- Supports disabling hot standby feedback by sending invalid transaction IDs
- When not using slots, stores the more restrictive of regular and catalog xmin values
- Provides comprehensive debug logging for troubleshooting replication issues
- Located in src/backend/replication/walsender.c:2591-2714