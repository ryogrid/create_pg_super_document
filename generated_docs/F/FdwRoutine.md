# FdwRoutine

## Location
[src/include/foreign/fdwapi.h:204-281](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/foreign/fdwapi.h#L204-L281)

## Overview
FdwRoutine is a structure that defines the callback function interface for Foreign Data Wrappers (FDWs) in PostgreSQL, providing all the function pointers needed by the planner and executor to interact with foreign tables.

## Definition

```c
typedef struct FdwRoutine
{
	NodeTag		type;

	/* Functions for scanning foreign tables */
	GetForeignRelSize_function GetForeignRelSize;
	GetForeignPaths_function GetForeignPaths;
	GetForeignPlan_function GetForeignPlan;
	BeginForeignScan_function BeginForeignScan;
	IterateForeignScan_function IterateForeignScan;
	ReScanForeignScan_function ReScanForeignScan;
	EndForeignScan_function EndForeignScan;

	/*
	 * Remaining functions are optional.  Set the pointer to NULL for any that
	 * are not provided.
	 */

	/* Functions for remote-join planning */
	GetForeignJoinPaths_function GetForeignJoinPaths;

	/* Functions for remote upper-relation (post scan/join) planning */
	GetForeignUpperPaths_function GetForeignUpperPaths;

	/* Functions for updating foreign tables */
	AddForeignUpdateTargets_function AddForeignUpdateTargets;
	PlanForeignModify_function PlanForeignModify;
	BeginForeignModify_function BeginForeignModify;
	ExecForeignInsert_function ExecForeignInsert;
	ExecForeignBatchInsert_function ExecForeignBatchInsert;
	GetForeignModifyBatchSize_function GetForeignModifyBatchSize;
	ExecForeignUpdate_function ExecForeignUpdate;
	ExecForeignDelete_function ExecForeignDelete;
	EndForeignModify_function EndForeignModify;
	BeginForeignInsert_function BeginForeignInsert;
	EndForeignInsert_function EndForeignInsert;
	IsForeignRelUpdatable_function IsForeignRelUpdatable;
	PlanDirectModify_function PlanDirectModify;
	BeginDirectModify_function BeginDirectModify;
	IterateDirectModify_function IterateDirectModify;
	EndDirectModify_function EndDirectModify;

	/* Functions for SELECT FOR UPDATE/SHARE row locking */
	GetForeignRowMarkType_function GetForeignRowMarkType;
	RefetchForeignRow_function RefetchForeignRow;
	RecheckForeignScan_function RecheckForeignScan;

	/* Support functions for EXPLAIN */
	ExplainForeignScan_function ExplainForeignScan;
	ExplainForeignModify_function ExplainForeignModify;
	ExplainDirectModify_function ExplainDirectModify;

	/* Support functions for ANALYZE */
	AnalyzeForeignTable_function AnalyzeForeignTable;

	/* Support functions for IMPORT FOREIGN SCHEMA */
	ImportForeignSchema_function ImportForeignSchema;

	/* Support functions for TRUNCATE */
	ExecForeignTruncate_function ExecForeignTruncate;

	/* Support functions for parallelism under Gather node */
	IsForeignScanParallelSafe_function IsForeignScanParallelSafe;
	EstimateDSMForeignScan_function EstimateDSMForeignScan;
	InitializeDSMForeignScan_function InitializeDSMForeignScan;
	ReInitializeDSMForeignScan_function ReInitializeDSMForeignScan;
	InitializeWorkerForeignScan_function InitializeWorkerForeignScan;
	ShutdownForeignScan_function ShutdownForeignScan;

	/* Support functions for path reparameterization. */
	ReparameterizeForeignPathByChild_function ReparameterizeForeignPathByChild;

	/* Support functions for asynchronous execution */
	IsForeignPathAsyncCapable_function IsForeignPathAsyncCapable;
	ForeignAsyncRequest_function ForeignAsyncRequest;
	ForeignAsyncConfigureWait_function ForeignAsyncConfigureWait;
	ForeignAsyncNotify_function ForeignAsyncNotify;
} FdwRoutine;
```
## Detailed Description
FdwRoutine serves as the primary interface between PostgreSQL's core execution engine and Foreign Data Wrappers. This structure is returned by an FDW's handler function and contains function pointers that the PostgreSQL planner and executor use to interact with foreign data sources. The structure is designed to be extensible, with the recommendation that handlers initialize it using makeNode(FdwRoutine) to ensure all fields are set to NULL by default.

The structure is organized into several functional groups: basic scanning operations (required), modification operations, locking support, explain functionality, analysis support, schema import, truncation, parallelism, path reparameterization, and asynchronous execution capabilities. Most functions beyond the basic scanning operations are optional and can be set to NULL if not supported by the FDW.

## Parameters / Member Variables
- `type`: Standard PostgreSQL NodeTag for type identification
- `GetForeignRelSize`: Required function to estimate the size of a foreign relation for planning
- `GetForeignPaths`: Required function to create access paths for scanning a foreign relation
- `GetForeignPlan`: Required function to create a ForeignScan plan node
- `BeginForeignScan`: Required function to initialize scanning of a foreign relation
- `IterateForeignScan`: Required function to fetch the next tuple from a foreign relation
- `ReScanForeignScan`: Required function to restart scanning from the beginning
- `EndForeignScan`: Required function to clean up after scanning
- `GetForeignJoinPaths`: Optional function for remote join planning
- `GetForeignUpperPaths`: Optional function for remote upper-relation planning
- `AddForeignUpdateTargets`: Optional function to add resjunk columns needed for UPDATE/DELETE
- `PlanForeignModify`: Optional function to plan foreign table modifications
- `BeginForeignModify`: Optional function to initialize foreign table modifications
- `ExecForeignInsert`: Optional function to execute foreign table inserts
- `ExecForeignBatchInsert`: Optional function to execute batch inserts
- `GetForeignModifyBatchSize`: Optional function to determine optimal batch size
- `ExecForeignUpdate`: Optional function to execute foreign table updates
- `ExecForeignDelete`: Optional function to execute foreign table deletes
- `EndForeignModify`: Optional function to clean up after modifications
- `BeginForeignInsert`: Optional function to initialize foreign table inserts
- `EndForeignInsert`: Optional function to clean up after inserts
- `IsForeignRelUpdatable`: Optional function to check if a foreign relation is updatable
- `PlanDirectModify`: Optional function to plan direct foreign table modifications
- `BeginDirectModify`: Optional function to initialize direct modifications
- `IterateDirectModify`: Optional function to execute direct modifications
- `EndDirectModify`: Optional function to clean up after direct modifications
- `GetForeignRowMarkType`: Optional function to determine row locking strategy
- `RefetchForeignRow`: Optional function to refetch a row for locking
- `RecheckForeignScan`: Optional function to recheck visibility after locking
- `ExplainForeignScan`: Optional function to provide EXPLAIN output for scans
- `ExplainForeignModify`: Optional function to provide EXPLAIN output for modifications
- `ExplainDirectModify`: Optional function to provide EXPLAIN output for direct modifications
- `AnalyzeForeignTable`: Optional function to support ANALYZE on foreign tables
- `ImportForeignSchema`: Optional function to support IMPORT FOREIGN SCHEMA
- `ExecForeignTruncate`: Optional function to support TRUNCATE on foreign tables
- `IsForeignScanParallelSafe`: Optional function to check if scanning is parallel-safe
- `EstimateDSMForeignScan`: Optional function to estimate dynamic shared memory for parallel scans
- `InitializeDSMForeignScan`: Optional function to initialize DSM for parallel scans
- `ReInitializeDSMForeignScan`: Optional function to reinitialize DSM for parallel scans
- `InitializeWorkerForeignScan`: Optional function to initialize worker processes
- `ShutdownForeignScan`: Optional function to shut down parallel scanning
- `ReparameterizeForeignPathByChild`: Optional function for path reparameterization
- `IsForeignPathAsyncCapable`: Optional function to check asynchronous execution capability
- `ForeignAsyncRequest`: Optional function to request asynchronous execution
- `ForeignAsyncConfigureWait`: Optional function to configure waiting for async operations
- `ForeignAsyncNotify`: Optional function to handle async operation notifications
## Dependencies
- Functions called/Symbols referenced:
  - [ImportForeignSchema](../I/ImportForeignSchema.md)
- Called from (representative examples):
  - [GetFdwRoutine](../G/GetFdwRoutine.md)
  - [GetFdwRoutineByServerId](../G/GetFdwRoutineByServerId.md)
  - [GetFdwRoutineByRelId](../G/GetFdwRoutineByRelId.md)
  - [GetFdwRoutineForRelation](../G/GetFdwRoutineForRelation.md)
  - [ExecInitForeignScan](../E/ExecInitForeignScan.md)
  - make_modifytable
  - [select_rowmark_type](../s/select_rowmark_type.md)
  - [analyze_rel](../a/analyze_rel.md)
  - [show_foreignscan_info](../s/show_foreignscan_info.md)
  - [ExecuteTruncateGuts](../E/ExecuteTruncateGuts.md)

## Notes and Other Information
The FdwRoutine structure is central to PostgreSQL's Foreign Data Wrapper architecture and serves as the contract between FDW implementations and the core database engine. FDW developers must implement at least the basic scanning functions, while other functions can be implemented as needed based on the capabilities of the foreign data source. The structure's design allows for future extensibility without breaking existing FDW implementations, as new function pointers can be added and existing FDWs will have them initialized to NULL by makeNode().