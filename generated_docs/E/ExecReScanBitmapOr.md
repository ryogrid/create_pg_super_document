# ExecReScanBitmapOr

## Location
[src/backend/executor/nodeBitmapOr.c:219-241](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeBitmapOr.c#L219-L241)

## Overview
ExecReScanBitmapOr handles parameter changes and initiates rescanning of all child subplans when the BitmapOr node needs to be re-executed.

## Definition


## Detailed Description
ExecReScanBitmapOr manages the rescan operation for BitmapOr nodes, which is necessary when query parameters change during execution (typically in nested loop scenarios). The function is responsible for propagating parameter change information to child subplans and ensuring they are properly rescanned when needed.

The function implements an important optimization: it manually handles parameter change signaling since the standard ExecReScan mechanism doesn't automatically know about BitmapOr's subplans. For each child subplan, it updates the changed parameter set using UpdateChangedParamSet, then conditionally calls ExecReScan only if the subplan doesn't have pending parameter changes (since subplans with pending changes will be automatically re-scanned on their first execution).

This approach avoids unnecessary rescan operations while ensuring that all relevant subplans are properly notified of parameter changes and will be re-executed when needed.

## Parameters / Member Variables
- : Pointer to the BitmapOrState structure containing the child subplans to be rescanned

## Dependencies
- Functions called/Symbols referenced:
  - [UpdateChangedParamSet](../U/UpdateChangedParamSet.md) (propagates parameter changes to child subplans)
  - [ExecReScan](ExecReScan.md) (conditionally rescans child subplans)

- Called from (representative examples):
  - [ExecReScan](ExecReScan.md) (part of the general rescan dispatch system)

## Notes and Other Information
- Manually handles parameter change signaling since ExecReScan doesn't know about BitmapOr's subplans
- Uses UpdateChangedParamSet to efficiently propagate only relevant parameter changes
- Implements conditional rescanning to avoid redundant operations
- Essential for correct execution in nested loop joins where outer parameters change
- The function references the already processed symbol UpdateChangedParamSet for parameter management
- Does not return a value as it performs state updates rather than data processing