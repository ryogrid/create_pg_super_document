# spgLeafTest

## Location
src/backend/access/spgist/spgscan.c: 516 - 605

## Overview
Tests whether a leaf tuple satisfies all scan keys and handles the result appropriately for both ordered and non-ordered SP-GiST index scans.

## Definition


## Detailed Description
This function is the core leaf tuple evaluation mechanism in SP-GiST scanning. It determines whether a leaf tuple matches the scan conditions by calling the opclass-specific leaf_consistent function. The function handles both NULL and non-NULL leaf values, manages memory contexts properly, and processes results differently depending on whether the scan is ordered or not.

For ordered scans (with ORDER BY clauses), qualifying tuples are added to a priority queue for later retrieval in distance order. For non-ordered scans, qualifying tuples are immediately reported to the result store function.

The function carefully manages memory contexts, using temporary context for the leaf_consistent call and switching to traversal context when creating heap items for the queue.

## Parameters
- : SpGistScanOpaque - The scan state containing keys, configuration, and context information
- : SpGistSearchItem * - The current search item (should be an inner node item, not leaf)
- : SpGistLeafTuple - The leaf tuple being tested
- : bool - Whether the leaf tuple contains a NULL value
- : bool * - Output flag set to true if a non-ordered scan reports a result
- : storeRes_func - Function pointer for storing results in non-ordered scans

## Dependencies
- Functions called/Symbols referenced:
  - SGLTDATUM - Extracts datum from leaf tuple
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md) - Calls the leaf_consistent function
  - [spgNewHeapItem](spgNewHeapItem.md) - Creates heap items for ordered scan results
  - [spgAddSearchItemToQueue](spgAddSearchItemToQueue.md) - Adds items to the priority queue
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) - Manages memory contexts
- Called from:
  - [spgTestLeafTuple](spgTestLeafTuple.md) - Tests individual leaf tuples during page scanning

## Notes and Other Information
- The function includes an assertion that the input item should not be a leaf item itself
- NULL handling is straightforward - if nulls are being searched for, NULL tuples automatically qualify
- Memory context management is critical to prevent leaks during leaf_consistent calls
- The function handles both immediate result reporting (non-ordered) and queue-based processing (ordered)
- Distance calculations and recheck flags are properly propagated from the leaf_consistent function
- The reconstructedValue from the traversal path is passed to leaf_consistent for proper evaluation