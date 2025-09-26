# Tuplesortstate

## Location
[src/backend/utils/sort/tuplesort.c:186-345](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L186-L345)

## Overview
Tuplesortstate is the core private state structure for PostgreSQL's tuple sorting operations, managing all aspects of tuple sorting including memory management, disk-based merging, parallel coordination, and optimization strategies.

## Definition

```c
struct Tuplesortstate
{
	TuplesortPublic base;
	TupSortStatus status;		/* enumerated value as shown above */
	bool		bounded;		/* did caller specify a maximum number of
								 * tuples to return? */
	bool		boundUsed;		/* true if we made use of a bounded heap */
	int			bound;			/* if bounded, the maximum number of tuples */
	int64		tupleMem;		/* memory consumed by individual tuples.
								 * storing this separately from what we track
								 * in availMem allows us to subtract the
								 * memory consumed by all tuples when dumping
								 * tuples to tape */
	int64		availMem;		/* remaining memory available, in bytes */
	int64		allowedMem;		/* total memory allowed, in bytes */
	int			maxTapes;		/* max number of input tapes to merge in each
								 * pass */
	int64		maxSpace;		/* maximum amount of space occupied among sort
								 * of groups, either in-memory or on-disk */
	bool		isMaxSpaceDisk; /* true when maxSpace is value for on-disk
								 * space, false when it's value for in-memory
								 * space */
	TupSortStatus maxSpaceStatus;	/* sort status when maxSpace was reached */
	LogicalTapeSet *tapeset;	/* logtape.c object for tapes in a temp file */

	/*
	 * This array holds the tuples now in sort memory.  If we are in state
	 * INITIAL, the tuples are in no particular order; if we are in state
	 * SORTEDINMEM, the tuples are in final sorted order; in states BUILDRUNS
	 * and FINALMERGE, the tuples are organized in "heap" order per Algorithm
	 * H.  In state SORTEDONTAPE, the array is not used.
	 */
	SortTuple  *memtuples;		/* array of SortTuple structs */
	int			memtupcount;	/* number of tuples currently present */
	int			memtupsize;		/* allocated length of memtuples array */
	bool		growmemtuples;	/* memtuples' growth still underway? */

	/*
	 * Memory for tuples is sometimes allocated using a simple slab allocator,
	 * rather than with palloc().  Currently, we switch to slab allocation
	 * when we start merging.  Merging only needs to keep a small, fixed
	 * number of tuples in memory at any time, so we can avoid the
	 * palloc/pfree overhead by recycling a fixed number of fixed-size slots
	 * to hold the tuples.
	 *
	 * For the slab, we use one large allocation, divided into SLAB_SLOT_SIZE
	 * slots.  The allocation is sized to have one slot per tape, plus one
	 * additional slot.  We need that many slots to hold all the tuples kept
	 * in the heap during merge, plus the one we have last returned from the
	 * sort, with tuplesort_gettuple.
	 *
	 * Initially, all the slots are kept in a linked list of free slots.  When
	 * a tuple is read from a tape, it is put to the next available slot, if
	 * it fits.  If the tuple is larger than SLAB_SLOT_SIZE, it is palloc'd
	 * instead.
	 *
	 * When we're done processing a tuple, we return the slot back to the free
	 * list, or pfree() if it was palloc'd.  We know that a tuple was
	 * allocated from the slab, if its pointer value is between
	 * slabMemoryBegin and -End.
	 *
	 * When the slab allocator is used, the USEMEM/LACKMEM mechanism of
	 * tracking memory usage is not used.
	 */
	bool		slabAllocatorUsed;

	char	   *slabMemoryBegin;	/* beginning of slab memory arena */
	char	   *slabMemoryEnd;	/* end of slab memory arena */
	SlabSlot   *slabFreeHead;	/* head of free list */

	/* Memory used for input and output tape buffers. */
	size_t		tape_buffer_mem;

	/*
	 * When we return a tuple to the caller in tuplesort_gettuple_XXX, that
	 * came from a tape (that is, in TSS_SORTEDONTAPE or TSS_FINALMERGE
	 * modes), we remember the tuple in 'lastReturnedTuple', so that we can
	 * recycle the memory on next gettuple call.
	 */
	void	   *lastReturnedTuple;

	/*
	 * While building initial runs, this is the current output run number.
	 * Afterwards, it is the number of initial runs we made.
	 */
	int			currentRun;

	/*
	 * Logical tapes, for merging.
	 *
	 * The initial runs are written in the output tapes.  In each merge pass,
	 * the output tapes of the previous pass become the input tapes, and new
	 * output tapes are created as needed.  When nInputTapes equals
	 * nInputRuns, there is only one merge pass left.
	 */
	LogicalTape **inputTapes;
	int			nInputTapes;
	int			nInputRuns;

	LogicalTape **outputTapes;
	int			nOutputTapes;
	int			nOutputRuns;

	LogicalTape *destTape;		/* current output tape */

	/*
	 * These variables are used after completion of sorting to keep track of
	 * the next tuple to return.  (In the tape case, the tape's current read
	 * position is also critical state.)
	 */
	LogicalTape *result_tape;	/* actual tape of finished output */
	int			current;		/* array index (only used if SORTEDINMEM) */
	bool		eof_reached;	/* reached EOF (needed for cursors) */

	/* markpos_xxx holds marked position for mark and restore */
	int64		markpos_block;	/* tape block# (only used if SORTEDONTAPE) */
	int			markpos_offset; /* saved "current", or offset in tape block */
	bool		markpos_eof;	/* saved "eof_reached" */

	/*
	 * These variables are used during parallel sorting.
	 *
	 * worker is our worker identifier.  Follows the general convention that
	 * -1 value relates to a leader tuplesort, and values >= 0 worker
	 * tuplesorts. (-1 can also be a serial tuplesort.)
	 *
	 * shared is mutable shared memory state, which is used to coordinate
	 * parallel sorts.
	 *
	 * nParticipants is the number of worker Tuplesortstates known by the
	 * leader to have actually been launched, which implies that they must
	 * finish a run that the leader needs to merge.  Typically includes a
	 * worker state held by the leader process itself.  Set in the leader
	 * Tuplesortstate only.
	 */
	int			worker;
	Sharedsort *shared;
	int			nParticipants;

	/*
	 * Additional state for managing "abbreviated key" sortsupport routines
	 * (which currently may be used by all cases except the hash index case).
	 * Tracks the intervals at which the optimization's effectiveness is
	 * tested.
	 */
	int64		abbrevNext;		/* Tuple # at which to next check
								 * applicability */

	/*
	 * Resource snapshot for time of sort start.
	 */
#ifdef TRACE_SORT
	PGRUsage	ru_start;
#endif
};
```
## Detailed Description
Tuplesortstate is the central data structure that orchestrates PostgreSQL's sophisticated tuple sorting system. It manages the complete lifecycle of sorting operations from initial tuple collection through various optimization strategies to final result delivery.

The structure supports multiple sorting phases:
- **Initial Phase**: Collects tuples in memory using the memtuples array
- **Memory Sorting**: When memory limits are reached, switches to heap-based or quicksort-based sorting
- **External Sorting**: For large datasets, creates runs on tape storage and performs multi-way merges
- **Bounded Heap**: For TOP-K queries, maintains a bounded heap for efficiency
- **Parallel Coordination**: Coordinates multiple worker processes in parallel sorting scenarios

The sorting system employs several memory management strategies including slab allocation during merge phases to reduce palloc/pfree overhead, and sophisticated memory tracking to balance between in-memory and disk-based operations.

## Parameters / Member Variables
- : Public interface structure containing shared sorting functionality
- : Current phase of sorting operation (INITIAL, SORTEDINMEM, BUILDRUNS, FINALMERGE, SORTEDONTAPE)
- : Whether caller specified a maximum number of tuples to return
- : Whether bounded heap optimization was actually employed
- : Maximum number of tuples to return if bounded
- : Memory consumed by individual tuples, tracked separately for accurate accounting
- : Remaining memory available for sorting operations
- : Total memory budget allocated for this sort
- : Maximum number of input tapes that can be merged in a single pass
- : Peak space usage recorded during sorting (either memory or disk)
- : Whether maxSpace represents disk usage (true) or memory usage (false)
- : Sort phase when maximum space usage was recorded
- : Logical tape set for managing temporary files during external sorting
- : Array of SortTuple structs holding tuples in memory
- : Current number of tuples stored in memtuples array
- : Allocated capacity of memtuples array
- : Whether memtuples array is still growing or has reached final size
- : Whether slab allocation is active for memory management
- /: Boundaries of slab memory arena
- : Head of linked list tracking free slab slots
- : Memory allocated for tape I/O buffers
- : Last tuple returned to caller, held for memory recycling
- : Current output run number during run building, or total runs after completion
- /: Arrays of logical tapes for merge operations
- /: Number of input and output tapes
- /: Number of runs on input and output tapes
- : Current destination tape for output
- : Final tape containing sorted results
- : Current position index for in-memory sorted results
- : Whether end-of-file has been reached during result retrieval
- //: Saved position for mark/restore functionality
- : Worker identifier for parallel sorting (-1 for leader, ≥0 for workers)
- : Shared memory structure for coordinating parallel workers
- : Number of participating workers (maintained by leader)
- : Next tuple count at which to test abbreviated key optimization effectiveness

## Dependencies
- Functions called/Symbols referenced:
  - TuplesortPublic (base interface)
  - TupSortStatus (status enumeration)
  - [LogicalTapeSet](../L/LogicalTapeSet.md) (tape management)
  - SortTuple (tuple storage structure)
  - SlabSlot (slab allocation)
  - [LogicalTape](../L/LogicalTape.md) (individual tape operations)
  - [Sharedsort](../S/Sharedsort.md) (parallel coordination)
  - [PGRUsage](../P/PGRUsage.md) (resource usage tracking)

- Called from (representative examples):
  - [tuplesort_begin_heap](../t/tuplesort_begin_heap.md)
  - [tuplesort_begin_cluster](../t/tuplesort_begin_cluster.md)  
  - [tuplesort_begin_index_btree](../t/tuplesort_begin_index_btree.md)
  - [tuplesort_performsort](../t/tuplesort_performsort.md)
  - [tuplesort_gettuple_common](../t/tuplesort_gettuple_common.md)
  - [ExecSort](../E/ExecSort.md) (executor node)
  - [build_pertrans_for_aggref](../b/build_pertrans_for_aggref.md) (aggregate functions)

## Notes and Other Information
- The structure is designed to handle both small in-memory sorts and large external sorts seamlessly
- Slab allocation is employed during merge phases to reduce memory management overhead
- The bounded heap optimization is crucial for efficient TOP-K query processing
- Parallel sorting coordination allows multiple worker processes to contribute to a single large sort
- Abbreviated key support provides significant performance improvements for string and numeric sorting
- Memory tracking is sophisticated, separating tuple memory from infrastructure memory for accurate space management
- The design supports mark/restore operations for cursor-based result retrieval
- Resource usage tracking (when TRACE_SORT is enabled) helps with performance analysis and debugging