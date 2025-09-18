# validate_sync_standby_slots

## Location
src/backend/replication/slot.c: 2433 - 2487

## Overview
A helper function that validates slots specified in the GUC synchronized_standby_slots configuration parameter by parsing the input string and verifying each slot exists and is physical.

## Definition


## Detailed Description
This function validates the synchronized_standby_slots GUC parameter by first parsing the comma-separated list of slot names using SplitIdentifierString, then verifying that each specified slot exists and is a physical replication slot. The validation is performed under ReplicationSlotControlLock to ensure consistency. If any slot doesn't exist or is not physical, the function sets appropriate error details and returns false. The function skips slot existence checks for processes without a PGPROC structure, as explained in StandbySlotsHaveCaughtup() comments.

## Parameters / Member Variables
- `rawname`: The raw string containing comma-separated slot names to validate
- `elemlist`: Output parameter that receives the parsed list of slot names if validation succeeds

## Dependencies
- Functions called/Symbols referenced:
  - SplitIdentifierString (for parsing comma-separated identifiers)
  - GUC_check_errdetail (for setting error messages)
  - LWLockAcquire/LWLockRelease (for locking ReplicationSlotControlLock)
  - SearchNamedReplicationSlot (to find slots by name)
  - SlotIsPhysical (to check if slot is physical)
  - foreach_ptr (macro for iterating over list)
- Called from (representative examples):
  - check_synchronized_standby_slots

## Notes and Other Information
- This is a static function used internally within the slot.c module
- The function performs validation only when MyProc is available, skipping checks for processes without PGPROC
- Uses shared lock on ReplicationSlotControlLock to safely access slot information
- Returns false and sets detailed error messages via GUC_check_errdetail when validation fails
- Only accepts physical replication slots, rejecting logical slots