# PortalRunSelect

## Location
src/backend/tcop/pquery.c: 865 - 997

## Overview
Executes a portal's query in SELECT mode, handling both forward and backward sequential access to fetch tuples from either the executor or a held store.

## Definition
static uint64 PortalRunSelect(Portal portal, bool forward, long count, DestReceiver *dest)

## Detailed Description
PortalRunSelect is the core function for executing SELECT-style queries through portals, supporting both PORTAL_ONE_SELECT mode and fetching from completed holdStore in PORTAL_ONE_RETURNING, PORTAL_ONE_MOD_WITH, and PORTAL_UTIL_SELECT cases. It handles simple N-rows-forward-or-backward access patterns and manages portal position state.

The function determines scan direction based on the forward parameter and current portal state, then either runs the executor directly or fetches from a held tuple store. It carefully manages portal position markers (atStart, atEnd, portalPos) and validates scroll capabilities for backward scans. The function supports both live execution through ExecutorRun and fetching from stored results through RunFromStore.

## Parameters / Member Variables
- portal: The Portal to execute, must have either a ready queryDesc or holdStore
- forward: true for forward scan, false for backward scan
- count: Maximum number of rows to fetch; FETCH_ALL means all rows, count <= 0 is a no-op
- dest: DestReceiver where fetched tuples should be sent

## Dependencies
- Functions called/Symbols referenced:
  - RunFromStore
  - PushActiveSnapshot
  - ExecutorRun
  - PopActiveSnapshot
  - ScanDirectionIsNoMovement
- Called from (representative examples):
  - PortalRun
  - DoPortalRunFetch (multiple locations)

## Notes and Other Information
- Returns the number of rows processed, suitable for use in result tags
- Handles both live query execution and fetching from held cursor data
- Validates scroll permissions for backward scans, requiring CURSOR_OPT_SCROLL
- Manages portal position state including atStart, atEnd, and portalPos counters
- Forces queryDesc destination to match the provided dest parameter on each call
- Uses NoMovementScanDirection when already at boundary or count <= 0
- Supports FETCH_ALL by converting to count = 0 for the executor
- Located in src/backend/tcop/pquery.c:865-997