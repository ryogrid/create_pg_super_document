# make_tidrangescan

## Location
src/backend/optimizer/plan/createplan.c: 5665 - 5683

## Overview
Creates and initializes a TidRangeScan plan node that scans a range of tuples in a heap table using tuple identifier (TID) range conditions, providing efficient access to contiguous blocks of tuples.

## Definition


## Detailed Description
This function constructs a TidRangeScan plan node, which implements a specialized scan operation that accesses tuples within specified TID ranges from a heap table. Unlike the single TID access provided by TidScan, TidRangeScan can efficiently process range conditions on TIDs, such as "WHERE ctid >= '(0,1)' AND ctid <= '(0,100)'". This allows for efficient scanning of contiguous blocks of tuples without requiring a full sequential scan, making it particularly useful for operations that need to process specific ranges of a table's physical storage.

## Parameters / Member Variables
- : Target list of expressions to be computed and returned by this scan
- : Additional qualification conditions to be evaluated against retrieved tuples
- : Range table index of the heap relation being scanned
- : List of qualification conditions that specify the TID range boundaries

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create the TidRangeScan node)
- Called from (representative examples):
  - [create_tidrangescan_plan](../c/create_tidrangescan_plan.md)

## Notes and Other Information
- This is a static function within createplan.c for internal plan construction
- TID range scans are useful for queries with range conditions on the CTID system column
- More efficient than sequential scans when only a specific range of physical tuple locations needs to be accessed
- The tidrangequals parameter contains range conditions that define the TID boundaries to scan
- No child plan nodes are needed since TID range scans access tuples directly based on physical address ranges
- Provides a middle ground between single-TID access (TidScan) and full table access (SeqScan)