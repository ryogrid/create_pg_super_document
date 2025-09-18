# assign_synchronized_standby_slots

## Location
src/backend/replication/slot.c: 2544 - 2558

## Overview
GUC assign_hook for the synchronized_standby_slots configuration parameter that updates global configuration and resets cached LSN values when the parameter changes.

## Definition


## Detailed Description
This function serves as a GUC (Grand Unified Configuration) assign hook for the synchronized_standby_slots parameter. It is called after successful validation when the parameter value is being applied. The function updates the global synchronized_standby_slots_config pointer with the new configuration data prepared by the check_hook, and resets ss_oldest_flush_lsn to InvalidXLogRecPtr to force recomputation of the oldest LSN among standby slots, since the set of synchronized standby slots may have changed.

## Parameters / Member Variables
- `newval`: The new GUC value string (unused in this function)
- `extra`: The SyncStandbySlotsConfigData structure prepared by check_synchronized_standby_slots

## Dependencies
- Functions called/Symbols referenced:
  - SyncStandbySlotsConfigData (configuration structure type)
  - ss_oldest_flush_lsn (global variable for cached oldest LSN)
  - InvalidXLogRecPtr (constant for invalid LSN)
  - synchronized_standby_slots_config (global configuration pointer)
- Called from (representative examples):
  - GUC system (referenced in guc_hooks.h)

## Notes and Other Information
- This function is called by PostgreSQL's GUC system after the check_hook has successfully validated the new parameter value
- Resets the cached oldest flush LSN to force recomputation when standby slots change
- The extra parameter contains the pre-validated and pre-formatted configuration data
- This is a simple assignment function that updates global state to reflect the new configuration
- Part of the GUC hook mechanism that ensures proper handling of configuration changes