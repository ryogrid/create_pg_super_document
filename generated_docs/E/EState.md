# EState

## Location
[src/include/nodes/execnodes.h:621-728](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L621-L728)

## Overview
EState (Executor State) is the central working state structure for PostgreSQL's query executor, containing all the runtime information needed during query execution.

## Definition

```c
typedef struct EState
{
	NodeTag		type;

	/* Basic state for all query types: */
	ScanDirection es_direction; /* current scan direction */
	Snapshot	es_snapshot;	/* time qual to use */
	Snapshot	es_crosscheck_snapshot; /* crosscheck time qual for RI */
	List	   *es_range_table; /* List of RangeTblEntry */
	Index		es_range_table_size;	/* size of the range table arrays */
	Relation   *es_relations;	/* Array of per-range-table-entry Relation
								 * pointers, or NULL if not yet opened */
	struct ExecRowMark **es_rowmarks;	/* Array of per-range-table-entry
										 * ExecRowMarks, or NULL if none */
	List	   *es_rteperminfos;	/* List of RTEPermissionInfo */
	PlannedStmt *es_plannedstmt;	/* link to top of plan tree */
	const char *es_sourceText;	/* Source text from QueryDesc */

	JunkFilter *es_junkFilter;	/* top-level junk filter, if any */

	/* If query can insert/delete tuples, the command ID to mark them with */
	CommandId	es_output_cid;

	/* Info about target table(s) for insert/update/delete queries: */
	ResultRelInfo **es_result_relations;	/* Array of per-range-table-entry
											 * ResultRelInfo pointers, or NULL
											 * if not a target table */
	List	   *es_opened_result_relations; /* List of non-NULL entries in
											 * es_result_relations in no
											 * specific order */

	PartitionDirectory es_partition_directory;	/* for PartitionDesc lookup */

	/*
	 * The following list contains ResultRelInfos created by the tuple routing
	 * code for partitions that aren't found in the es_result_relations array.
	 */
	List	   *es_tuple_routing_result_relations;

	/* Stuff used for firing triggers: */
	List	   *es_trig_target_relations;	/* trigger-only ResultRelInfos */

	/* Parameter info: */
	ParamListInfo es_param_list_info;	/* values of external params */
	ParamExecData *es_param_exec_vals;	/* values of internal params */

	QueryEnvironment *es_queryEnv;	/* query environment */

	/* Other working state: */
	MemoryContext es_query_cxt; /* per-query context in which EState lives */

	List	   *es_tupleTable;	/* List of TupleTableSlots */

	uint64		es_processed;	/* # of tuples processed during one
								 * ExecutorRun() call. */
	uint64		es_total_processed; /* total # of tuples aggregated across all
									 * ExecutorRun() calls. */

	int			es_top_eflags;	/* eflags passed to ExecutorStart */
	int			es_instrument;	/* OR of InstrumentOption flags */
	bool		es_finished;	/* true when ExecutorFinish is done */

	List	   *es_exprcontexts;	/* List of ExprContexts within EState */

	List	   *es_subplanstates;	/* List of PlanState for SubPlans */

	List	   *es_auxmodifytables; /* List of secondary ModifyTableStates */

	/*
	 * this ExprContext is for per-output-tuple operations, such as constraint
	 * checks and index-value computations.  It will be reset for each output
	 * tuple.  Note that it will be created only if needed.
	 */
	ExprContext *es_per_tuple_exprcontext;

	/*
	 * If not NULL, this is an EPQState's EState. This is a field in EState
	 * both to allow EvalPlanQual aware executor nodes to detect that they
	 * need to perform EPQ related work, and to provide necessary information
	 * to do so.
	 */
	struct EPQState *es_epq_active;

	bool		es_use_parallel_mode;	/* can we use parallel workers? */

	/* The per-query shared memory area to use for parallel execution. */
	struct dsa_area *es_query_dsa;

	/*
	 * JIT information. es_jit_flags indicates whether JIT should be performed
	 * and with which options.  es_jit is created on-demand when JITing is
	 * performed.
	 *
	 * es_jit_worker_instr is the combined, on demand allocated,
	 * instrumentation from all workers. The leader's instrumentation is kept
	 * separate, and is combined on demand by ExplainPrintJITSummary().
	 */
	int			es_jit_flags;
	struct JitContext *es_jit;
	struct JitInstrumentation *es_jit_worker_instr;

	/*
	 * Lists of ResultRelInfos for foreign tables on which batch-inserts are
	 * to be executed and owning ModifyTableStates, stored in the same order.
	 */
	List	   *es_insert_pending_result_relations;
	List	   *es_insert_pending_modifytables;
} EState;
```
## Detailed Description
EState serves as the comprehensive execution context for PostgreSQL queries, maintaining all runtime state needed during query execution. It bridges the gap between planning and execution by holding references to planned statements, managing runtime parameters, tracking tuple processing statistics, and coordinating resource management. The structure is designed to support complex execution scenarios including parallel execution, JIT compilation, triggers, and partitioning.

## Parameters / Member Variables
- `type`: NodeTag identifier for the structure
- `es_direction`: Current scan direction (forward/backward)
- `es_snapshot`: Snapshot for visibility checks during execution
- `es_crosscheck_snapshot`: Snapshot for referential integrity crosschecks
- `*es_range_table`: List of range table entries from the query
- `es_range_table_size`: Size of range table arrays
- `*es_relations`: Array of opened Relation pointers indexed by range table
- `**es_rowmarks`: Array of row locking information per range table entry
- `*es_rteperminfos`: List of permission information for range table entries
- `*es_plannedstmt`: Reference to the top-level planned statement
- `*es_sourceText`: Original SQL source text
- `*es_junkFilter`: Filter for removing junk attributes from result tuples
- `es_output_cid`: Command ID for marking inserted/deleted tuples
- `**es_result_relations`: Array of target relation information for DML operations
- `*es_opened_result_relations`: List of opened result relations
- `es_partition_directory`: Directory for partition descriptor lookups
- `*es_tuple_routing_result_relations`: Result relations created by tuple routing
- `*es_trig_target_relations`: Relations used only for trigger execution
- `es_param_list_info`: External parameter values
- `*es_param_exec_vals`: Internal executor parameter values
- `*es_queryEnv`: Query environment for accessing named result sets
- `es_query_cxt`: Memory context for per-query allocations
- `*es_tupleTable`: List of all TupleTableSlots used in execution
- `es_processed`: Number of tuples processed in current ExecutorRun call
- `es_total_processed`: Total tuples processed across all ExecutorRun calls
- `es_top_eflags`: Execution flags passed to ExecutorStart
- `es_instrument`: Instrumentation flags for performance monitoring
- `es_finished`: Flag indicating ExecutorFinish has completed
- `*es_exprcontexts`: List of expression evaluation contexts
- `*es_subplanstates`: List of plan states for subplans
- `*es_auxmodifytables`: List of auxiliary ModifyTable states
- `*es_per_tuple_exprcontext`: Expression context for per-tuple operations
- `*es_epq_active`: Active EPQ (EvalPlanQual) state for concurrent updates
- `es_use_parallel_mode`: Flag enabling parallel worker usage
- `*es_query_dsa`: Dynamic shared area for parallel execution coordination
- `es_jit_flags`: JIT compilation control flags
- `*es_jit`: JIT compilation context
- `*es_jit_worker_instr`: Combined instrumentation from parallel workers
- `*es_insert_pending_result_relations`: Relations with pending batch inserts
- `*es_insert_pending_modifytables`: ModifyTable states for batch inserts
## Dependencies
- Functions called/Symbols referenced:
  - ScanDirection (enum type)
  - [ExecRowMark](ExecRowMark.md) (struct type)
  - [PlannedStmt](../P/PlannedStmt.md) (struct type)
  - [JunkFilter](../J/JunkFilter.md) (struct type)
  - CommandId (type)
  - [PartitionDirectory](../P/PartitionDirectory.md) (struct type)
  - [ParamListInfo](../P/ParamListInfo.md) (struct type)
  - [ParamExecData](../P/ParamExecData.md) (struct type)
  - QueryEnvironment (struct type)
  - [EPQState](EPQState.md) (struct type)
  - dsa_area (struct type)
  - [JitContext](../J/JitContext.md) (struct type)
  - [JitInstrumentation](../J/JitInstrumentation.md) (struct type)
- Called from (representative examples):
  - [ExecutorStart](ExecutorStart.md) (creates and initializes EState)
  - [ExecutorRun](ExecutorRun.md) (operates on EState)
  - [ExecutorFinish](ExecutorFinish.md) (finalizes EState)

## Notes and Other Information
EState is the cornerstone of PostgreSQL's execution engine, created once per query execution and passed to all executor nodes. It supports advanced features like parallel execution through shared memory areas, JIT compilation for performance optimization, and complex DML operations with partitioning and triggers. The structure's design allows for incremental tuple processing and maintains comprehensive statistics for monitoring and optimization purposes.