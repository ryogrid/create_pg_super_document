# PlanState

## Location
[src/include/nodes/execnodes.h:1113-1205](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L1113-L1205)

## Overview
PlanState serves as the abstract base class for all plan node execution states in PostgreSQL, providing common infrastructure for query plan execution including tuple processing, expression evaluation, and runtime instrumentation.

## Definition

```c
typedef struct PlanState
{
	pg_node_attr(abstract)

	NodeTag		type;

	Plan	   *plan;			/* associated Plan node */

	EState	   *state;			/* at execution time, states of individual
								 * nodes point to one EState for the whole
								 * top-level plan */

	ExecProcNodeMtd ExecProcNode;	/* function to return next tuple */
	ExecProcNodeMtd ExecProcNodeReal;	/* actual function, if above is a
										 * wrapper */

	Instrumentation *instrument;	/* Optional runtime stats for this node */
	WorkerInstrumentation *worker_instrument;	/* per-worker instrumentation */

	/* Per-worker JIT instrumentation */
	struct SharedJitInstrumentation *worker_jit_instrument;

	/*
	 * Common structural data for all Plan types.  These links to subsidiary
	 * state trees parallel links in the associated plan tree (except for the
	 * subPlan list, which does not exist in the plan tree).
	 */
	ExprState  *qual;			/* boolean qual condition */
	struct PlanState *lefttree; /* input plan tree(s) */
	struct PlanState *righttree;

	List	   *initPlan;		/* Init SubPlanState nodes (un-correlated expr
								 * subselects) */
	List	   *subPlan;		/* SubPlanState nodes in my expressions */

	/*
	 * State for management of parameter-change-driven rescanning
	 */
	Bitmapset  *chgParam;		/* set of IDs of changed Params */

	/*
	 * Other run-time state needed by most if not all node types.
	 */
	TupleDesc	ps_ResultTupleDesc; /* node's return type */
	TupleTableSlot *ps_ResultTupleSlot; /* slot for my result tuples */
	ExprContext *ps_ExprContext;	/* node's expression-evaluation context */
	ProjectionInfo *ps_ProjInfo;	/* info for doing tuple projection */

	bool		async_capable;	/* true if node is async-capable */

	/*
	 * Scanslot's descriptor if known. This is a bit of a hack, but otherwise
	 * it's hard for expression compilation to optimize based on the
	 * descriptor, without encoding knowledge about all executor nodes.
	 */
	TupleDesc	scandesc;

	/*
	 * Define the slot types for inner, outer and scanslots for expression
	 * contexts with this state as a parent.  If *opsset is set, then
	 * *opsfixed indicates whether *ops is guaranteed to be the type of slot
	 * used. That means that every slot in the corresponding
	 * ExprContext.ecxt_*tuple will point to a slot of that type, while
	 * evaluating the expression.  If *opsfixed is false, but *ops is set,
	 * that indicates the most likely type of slot.
	 *
	 * The scan* fields are set by ExecInitScanTupleSlot(). If that's not
	 * called, nodes can initialize the fields themselves.
	 *
	 * If outer/inneropsset is false, the information is inferred on-demand
	 * using ExecGetResultSlotOps() on ->righttree/lefttree, using the
	 * corresponding node's resultops* fields.
	 *
	 * The result* fields are automatically set when ExecInitResultSlot is
	 * used (be it directly or when the slot is created by
	 * ExecAssignScanProjectionInfo() /
	 * ExecConditionalAssignProjectionInfo()).  If no projection is necessary
	 * ExecConditionalAssignProjectionInfo() defaults those fields to the scan
	 * operations.
	 */
	const TupleTableSlotOps *scanops;
	const TupleTableSlotOps *outerops;
	const TupleTableSlotOps *innerops;
	const TupleTableSlotOps *resultops;
	bool		scanopsfixed;
	bool		outeropsfixed;
	bool		inneropsfixed;
	bool		resultopsfixed;
	bool		scanopsset;
	bool		outeropsset;
	bool		inneropsset;
	bool		resultopsset;
} PlanState;
```
## Detailed Description
PlanState is the fundamental abstract base class for all execution state structures in PostgreSQL's executor. It provides the common framework that all plan node types inherit, containing essential execution infrastructure such as tuple processing functions, instrumentation for performance monitoring, expression evaluation contexts, and tree navigation pointers. This structure forms the backbone of PostgreSQL's execution engine, with specific node types like SeqScanState, NestLoopState, and HashJoinState all extending this base structure.

## Parameters / Member Variables
- `type`: NodeTag identifier for the specific PlanState subtype
- `plan`: Pointer to the associated Plan node from the plan tree
- `state`: Global execution state (EState) shared across the entire query execution
- `ExecProcNode`: Function pointer to retrieve the next tuple from this node
- `ExecProcNodeReal`: Actual processing function when ExecProcNode is a wrapper
- `instrument`: Runtime performance statistics collection for this node
- `worker_instrument`: Per-worker performance statistics for parallel execution
- `worker_jit_instrument`: Per-worker JIT compilation statistics
- `qual`: Expression state for boolean qualification conditions
- `lefttree`: Left child node in the execution tree
- `righttree`: Right child node in the execution tree
- `initPlan`: List of uncorrelated subplans executed during initialization
- `subPlan`: List of correlated subplans referenced in expressions
- `chgParam`: Set of parameter IDs that have changed, triggering rescans
- `ps_ResultTupleDesc`: Descriptor for tuples returned by this node
- `ps_ResultTupleSlot`: Slot for storing result tuples
- `ps_ExprContext`: Expression evaluation context for this node
- `ps_ProjInfo`: Projection information for tuple transformation
- `async_capable`: Flag indicating if node supports asynchronous execution
- `scandesc`: Tuple descriptor for scan slots (optimization hint)
- `scanops`, `outerops`, `innerops`, `resultops`: Slot operation types for different contexts
- `scanopsfixed`, `outeropsfixed`, `inneropsfixed`, `resultopsfixed`: Indicate whether corresponding slot types are guaranteed
- `scanopsset`, `outeropsset`, `inneropsset`, `resultopsset`: Indicate whether corresponding slot operation types are set

## Dependencies
- Functions called/Symbols referenced:
  - [Plan](Plan.md)
  - [EState](../E/EState.md)
  - ExecProcNodeMtd
  - Instrumentation
  - WorkerInstrumentation
  - [SharedJitInstrumentation](../S/SharedJitInstrumentation.md)
  - ExprState
  - [List](../L/List.md)
  - [Bitmapset](../B/Bitmapset.md)
  - [TupleDesc](../T/TupleDesc.md)
  - TupleTableSlot
  - ExprContext
  - [ProjectionInfo](ProjectionInfo.md)
  - TupleTableSlotOps
- Called from (representative examples):
  - All specific plan state structures (SeqScanState, NestLoopState, etc.)
  - [ExecInitNode](../E/ExecInitNode.md)
  - ExecProcNode
  - [ExecReScan](../E/ExecReScan.md)
  - [ExecEndNode](../E/ExecEndNode.md)

## Notes and Other Information
PlanState is never directly instantiated but serves as the common foundation for all executor node types. The structure includes sophisticated slot type management to optimize expression compilation and tuple processing. The async_capable flag enables PostgreSQL's asynchronous execution capabilities for improved parallelism. The instrumentation fields support PostgreSQL's query performance monitoring and EXPLAIN ANALYZE functionality. The dual function pointer design (ExecProcNode/ExecProcNodeReal) allows for wrapper functions that can add instrumentation or other cross-cutting concerns without affecting the core processing logic.