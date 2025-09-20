# TopoSort

## Location
[src/bin/pg_dump/pg_dump_sort.c:597-745](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump_sort.c#L597-L745)

## Overview
TopoSort performs a topological sort of a lock's wait queue to satisfy partial ordering constraints while minimizing disruption to the existing queue order.

## Definition

```c
structures here:
	 *
	 * processed[] is a bool array indexed by dump ID, marking the objects
	 * already processed during this invocation of findDependencyLoops().
	 *
	 * searchFailed[] is another array indexed by dump ID.  searchFailed[j] is
	 * set to dump ID k if we have proven that there is no dependency path
	 * leading from object j back to start point k.  This allows us to skip
	 * useless searching when there are multiple dependency paths from k to j,
	 * which is a common situation.  We could use a simple bool array for
	 * this, but then we'd need to re-zero it for each start point, resulting
	 * in O(N^2) zeroing work.  Using the start point's dump ID as the "true"
	 * value lets us skip clearing the array before we consider the next start
	 * point.
	 *
	 * workspace[] is an array of DumpableObject pointers, in which we try to
	 * build lists of objects constituting loops.  We make workspace[] large
	 * enough to hold all the objects in TopoSort's output, which is huge
	 * overkill in most cases but could theoretically be necessary if there is
	 * a single dependency chain linking all the objects.
	 */
	bool	   *processed;
```
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
  - [dclist_count](../d/dclist_count.md)
  - dclist_foreach
  - dlist_container
  - MemSet
- Called from (representative examples):
  - [ExpandConstraints](../E/ExpandConstraints.md)
  - [sortDumpableObjects](../s/sortDumpableObjects.md)

## Notes and Other Information
- Returns true if a valid ordering satisfying all constraints can be found, false if contradictory constraints exist
- Designed to be simpler than classical topological sort algorithms, trading efficiency for order preservation
- Handles lock groups by ensuring group members appear consecutively in output
- Uses global arrays topoProcs[], beforeConstraints[], and afterConstraints[] for processing
- Critical for deadlock resolution in PostgreSQL's lock management system
- The algorithm's apparent slowness is acceptable since it typically works with only a few constraints