# FillPortalStore

## Location
src/backend/tcop/pquery.c: 998 - 1058

## Overview
Runs a query and loads result tuples into the portal's tuple store for cases that require holding results in memory before delivery to the client.

## Definition


## Detailed Description
FillPortalStore is a specialized function used to execute queries and store their results in a portal's tuple store rather than streaming them directly to the client. This function is specifically designed for three portal strategies: PORTAL_ONE_RETURNING, PORTAL_ONE_MOD_WITH, and PORTAL_UTIL_SELECT. It creates a tuplestore destination receiver, configures it to write to the portal's hold store, and then executes the appropriate query execution path based on the portal's strategy. The function ensures that query results are captured and held in memory for later retrieval, which is essential for certain query patterns that require result buffering.

## Parameters / Member Variables
- : The Portal structure containing the query to execute and where results will be stored
- : Boolean flag indicating whether this is a top-level query execution

## Dependencies
- Functions called/Symbols referenced:
  - InitializeQueryCompletion
  - PortalCreateHoldStore
  - CreateDestReceiver
  - SetTuplestoreDestReceiverParams
  - PortalRunMulti
  - PortalRunUtility
  - CopyQueryCompletion
- Called from (representative examples):
  - PortalRun
  - PortalRunFetch

## Notes and Other Information
- This function is static and only used within pquery.c
- It handles three specific portal strategies, throwing an error for unsupported strategies
- The function sets up a tuplestore destination receiver to capture query output
- For PORTAL_ONE_RETURNING and PORTAL_ONE_MOD_WITH, it delegates to PortalRunMulti
- For PORTAL_UTIL_SELECT, it delegates to PortalRunUtility
- Query completion information is preserved and copied to the portal's completion data
- The destination receiver is properly destroyed after use to prevent memory leaks