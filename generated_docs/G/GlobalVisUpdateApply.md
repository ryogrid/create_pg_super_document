# GlobalVisUpdateApply

## Location
src/backend/storage/ipc/procarray.c: 4165 - 4203

## Overview
Applies computed transaction horizon results to update global visibility state boundaries for all relation types.

## Definition


## Detailed Description
This function updates the global visibility states for all relation types (shared, catalog, data, temp) with new horizon information computed by the transaction horizon calculation. It sets the maybe_needed boundaries based on the oldest non-removable transactions for each relation type, and ensures that definitely_needed boundaries are not earlier than maybe_needed boundaries.

The function handles the case where longer-running transactions that were previously considered running are no longer around, advancing the definitely_needed boundary accordingly. For temporary relations, the definitely_needed boundary is always set equal to maybe_needed.

## Parameters / Member Variables
- : Pointer to ComputeXidHorizonsResult structure containing computed horizon information for different relation types

## Dependencies
- Functions called/Symbols referenced:
  - FullXidRelativeTo
  - FullTransactionIdNewer
- Global visibility state variables updated:
  - GlobalVisSharedRels
  - GlobalVisCatalogRels
  - GlobalVisDataRels
  - GlobalVisTempRels
- Global variables:
  - ComputeXidHorizonsResultLastXmin
  - RecentXmin
- Called from:
  - ComputeXidHorizons
  - xc_slow_answer_inc

## Notes and Other Information
- This is a static function, only visible within procarray.c
- Updates both maybe_needed and definitely_needed boundaries for all relation types
- Ensures consistency by preventing definitely_needed from being earlier than maybe_needed
- Temporary relations have simplified logic where definitely_needed equals maybe_needed
- Records the RecentXmin value used for this update for future reference
- Critical for maintaining accurate global visibility state across the database cluster