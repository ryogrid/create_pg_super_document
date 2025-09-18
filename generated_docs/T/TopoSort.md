# TopoSort

## Location
src/bin/pg_dump/pg_dump_sort.c: 597 - 745

## Overview
TopoSort performs a topological sort of a lock's wait queue to satisfy partial ordering constraints while minimizing disruption to the existing queue order.

## Definition


## Detailed Description
TopoSort implements a topological sorting algorithm specifically designed for PostgreSQL's deadlock detection and resolution system. Unlike traditional topological sort algorithms (such as Knuth's), this implementation prioritizes preserving the existing queue order as much as possible while satisfying the given ordering constraints.

The algorithm operates on a lock's wait queue and processes a set of EDGE constraints that specify which processes must precede others. Each EDGE represents a dependency that needs to be reversed - the "waiter" must appear before the "blocker" in the output ordering.

The algorithm handles lock groups (introduced for parallel queries) by treating all members of a lock group as a unit, ensuring they appear consecutively in the output ordering. It uses a backwards scan approach, selecting processes with no remaining before-constraints and outputting entire lock groups at once.

## Parameters / Member Variables
- : The LOCK structure containing the wait queue to be reordered
- : Array of EDGE structures specifying partial ordering requirements  
- : Number of constraints in the constraints array
- : Output array of PGPROC pointers representing the reordered wait queue

## Dependencies
- Functions called/Symbols referenced:
  - dclist_count
  - dclist_foreach
  - dlist_container
  - MemSet
- Called from (representative examples):
  - ExpandConstraints
  - sortDumpableObjects

## Notes and Other Information
- Returns true if a valid ordering satisfying all constraints can be found, false if contradictory constraints exist
- Designed to be simpler than classical topological sort algorithms, trading efficiency for order preservation
- Handles lock groups by ensuring group members appear consecutively in output
- Uses global arrays topoProcs[], beforeConstraints[], and afterConstraints[] for processing
- Critical for deadlock resolution in PostgreSQL's lock management system
- The algorithm's apparent slowness is acceptable since it typically works with only a few constraints