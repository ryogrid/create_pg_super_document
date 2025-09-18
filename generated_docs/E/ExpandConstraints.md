# ExpandConstraints

## Location
[src/backend/storage/lmgr/deadlock.c:787-858](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/deadlock.c#L787-L858)

## Overview
ExpandConstraints expands a list of soft edge constraints into specific new orderings for affected lock wait queues, building data structures needed for deadlock resolution.

## Definition
static bool ExpandConstraints(EDGE *constraints, int nConstraints)

## Detailed Description
This function takes a set of soft edge constraints (edges that can be reversed to break deadlock cycles) and converts them into concrete wait queue orderings for the affected locks. It processes constraints in reverse order to test the most recently added constraint first, since that's the one most likely to fail due to inconsistencies.

For each unique lock referenced in the constraints, the function creates a WAIT_ORDER structure that will contain a reordered list of processes waiting for that lock. The actual reordering is delegated to the TopoSort function, which performs topological sorting to ensure the new ordering respects all constraints while avoiding contradictions.

The function allocates workspace in the global waitOrderProcs array and builds the waitOrders array that contains the proposed new orderings. These data structures are later used by the deadlock resolution mechanism to determine if rearranging wait queues can break the deadlock cycle.

## Parameters / Member Variables
- : Array of EDGE structures representing soft edges that could be reversed to break deadlock
- : Number of constraint edges in the constraints array

## Dependencies
- Functions called/Symbols referenced:
  - [dclist_count](../d/dclist_count.md)
  - [TopoSort](../T/TopoSort.md)
- Called from (representative examples):
  - [TestConfiguration](../T/TestConfiguration.md)

## Notes and Other Information
- Uses global arrays waitOrders and waitOrderProcs for workspace allocation
- Sets global variable nWaitOrders to track number of wait queue orderings created
- Processes constraints backwards to fail fast on the most recent constraint
- Returns false if TopoSort detects contradictory constraints that cannot be satisfied
- Each lock gets at most one reordered wait queue, even if multiple constraints affect it
- The workspace allocation ensures total process count doesn't exceed MaxBackends
- Critical component of PostgreSQL's deadlock resolution strategy that attempts to break cycles by reordering wait queues