# supportSecondarySplit

## Location
src/backend/access/gist/gistsplit.c: 258 - 343

## Overview
Handles cleanup and optimization after a secondary split when the user-defined PickSplit method didn't support secondary splits, leaving split datum flags set.

## Definition


## Detailed Description
This function is called to clean up when a secondary split was performed but the user-defined PickSplit method didn't support it, evidenced by spl_ldatum_exists or spl_rdatum_exists flags still being true. The function performs two main tasks:

1. **Split Optimization**: Evaluates whether to swap the left and right outputs of the secondary split by calculating penalties for merging tuples into the previously chosen sets. It chooses the configuration that minimizes the total penalty.

2. **Union Key Update**: Updates the union datums for the current column by incorporating the previous union keys (oldL/oldR), since the user-defined PickSplit method didn't handle this.

The function uses penalty calculations to determine the optimal arrangement and ensures that union keys properly reflect the combined data from both the original split and the secondary split.

## Parameters / Member Variables
- : The relation being operated on
- : GiST state information containing operator class methods
- : The attribute number (column) being processed
- : The split vector containing the results of the split operation
- : The previous left union key datum
- : The previous right union key datum

## Dependencies
- Functions called/Symbols referenced:
  - gistentryinit
  - gistpenalty
  - gistMakeUnionKey
  - SWAPVAR (macro)
- Types referenced:
  - GISTSTATE
  - GIST_SPLITVEC
  - GISTENTRY
- Called from:
  - gistUserPicksplit

## Notes and Other Information
- This function is only called when secondary splits occur and the user-defined PickSplit method lacks secondary split support
- The penalty-based optimization helps maintain efficient index structure by choosing the split arrangement with lower access costs
- The function handles edge cases where only one union key exists (when one side had all NULLs)
- After processing, it resets the spl_ldatum_exists and spl_rdatum_exists flags to false