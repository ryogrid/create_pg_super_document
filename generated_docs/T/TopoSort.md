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

## Simplified Source

```c
static bool
TopoSort(LOCK *lock, EDGE *constraints, int nConstraints, PGPROC **ordering)
{
    dclist_head *waitQueue = &lock->waitProcs;
    int queue_size = dclist_count(waitQueue);
    PGPROC *proc;
    int i, j, jj, k, kk, last;
    dlist_iter proc_iter;

    // Fill topoProcs[] with processes in current order
    i = 0;
    dclist_foreach(proc_iter, waitQueue) {
        proc = dlist_container(PGPROC, links, proc_iter.cur);
        topoProcs[i++] = proc;
    }

    // Initialize constraint tracking arrays
    MemSet(beforeConstraints, 0, queue_size * sizeof(int));
    MemSet(afterConstraints, 0, queue_size * sizeof(int));

    // Process each constraint to build dependency graph
    for (i = 0; i < nConstraints; i++) {
        // Find waiter process in queue
        proc = constraints[i].waiter;
        jj = -1;
        for (j = queue_size; --j >= 0;) {
            PGPROC *waiter = topoProcs[j];
            if (waiter == proc || waiter->lockGroupLeader == proc) {
                if (jj == -1)
                    jj = j;
                else
                    beforeConstraints[j] = -1; // Mark as group member
            }
        }

        if (jj < 0) continue; // Constraint not relevant

        // Find blocker process in queue
        proc = constraints[i].blocker;
        kk = -1;
        for (k = queue_size; --k >= 0;) {
            PGPROC *blocker = topoProcs[k];
            if (blocker == proc || blocker->lockGroupLeader == proc) {
                if (kk == -1)
                    kk = k;
                else
                    beforeConstraints[k] = -1; // Mark as group member
            }
        }

        if (kk < 0) continue; // Constraint not relevant

        // Update constraint counts and links
        beforeConstraints[jj]++;
        constraints[i].pred = jj;
        constraints[i].link = afterConstraints[kk];
        afterConstraints[kk] = i + 1;
    }

    // Generate output order by backwards scan
    last = queue_size - 1;
    for (i = queue_size - 1; i >= 0;) {
        int c, nmatches = 0;

        // Find next candidate with no remaining constraints
        while (topoProcs[last] == NULL)
            last--;
        for (j = last; j >= 0; j--) {
            if (topoProcs[j] != NULL && beforeConstraints[j] == 0)
                break;
        }

        if (j < 0) return false; // Contradictory constraints

        // Output entire lock group
        proc = topoProcs[j];
        if (proc->lockGroupLeader != NULL)
            proc = proc->lockGroupLeader;

        for (c = 0; c <= last; ++c) {
            if (topoProcs[c] == proc ||
                (topoProcs[c] != NULL && topoProcs[c]->lockGroupLeader == proc)) {
                ordering[i - nmatches] = topoProcs[c];
                topoProcs[c] = NULL;
                ++nmatches;
            }
        }
        i -= nmatches;

        // Update constraint counts for predecessors
        for (k = afterConstraints[j]; k > 0; k = constraints[k - 1].link)
            beforeConstraints[constraints[k - 1].pred]--;
    }

    return true;
}
```