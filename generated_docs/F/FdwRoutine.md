# FdwRoutine

## Location
src/include/foreign/fdwapi.h: 204 - 281

## Overview
FdwRoutine is a structure that defines the callback function interface for Foreign Data Wrappers (FDWs) in PostgreSQL, providing all the function pointers needed by the planner and executor to interact with foreign tables.

## Definition


## Detailed Description
FdwRoutine serves as the primary interface between PostgreSQL's core execution engine and Foreign Data Wrappers. This structure is returned by an FDW's handler function and contains function pointers that the PostgreSQL planner and executor use to interact with foreign data sources. The structure is designed to be extensible, with the recommendation that handlers initialize it using makeNode(FdwRoutine) to ensure all fields are set to NULL by default.

The structure is organized into several functional groups: basic scanning operations (required), modification operations, locking support, explain functionality, analysis support, schema import, truncation, parallelism, path reparameterization, and asynchronous execution capabilities. Most functions beyond the basic scanning operations are optional and can be set to NULL if not supported by the FDW.

## Parameters / Member Variables
- : Standard PostgreSQL NodeTag for type identification
- : Required function to estimate the size of a foreign relation for planning
- : Required function to create access paths for scanning a foreign relation
- : Required function to create a ForeignScan plan node
- : Required function to initialize scanning of a foreign relation
- : Required function to fetch the next tuple from a foreign relation
- : Required function to restart scanning from the beginning
- : Required function to clean up after scanning
- : Optional function for remote join planning
- : Optional function for remote upper-relation planning
- : Optional function to add resjunk columns needed for UPDATE/DELETE
- : Optional function to plan foreign table modifications
- : Optional function to initialize foreign table modifications
- : Optional function to execute foreign table inserts
- : Optional function to execute batch inserts
- : Optional function to determine optimal batch size
- : Optional function to execute foreign table updates
- : Optional function to execute foreign table deletes
- : Optional function to clean up after modifications
- : Optional function to initialize foreign table inserts
- : Optional function to clean up after inserts
- : Optional function to check if a foreign relation is updatable
- : Optional function to plan direct foreign table modifications
- : Optional function to initialize direct modifications
- : Optional function to execute direct modifications
- : Optional function to clean up after direct modifications
- : Optional function to determine row locking strategy
- : Optional function to refetch a row for locking
- : Optional function to recheck visibility after locking
- : Optional function to provide EXPLAIN output for scans
- : Optional function to provide EXPLAIN output for modifications
- : Optional function to provide EXPLAIN output for direct modifications
- : Optional function to support ANALYZE on foreign tables
- : Optional function to support IMPORT FOREIGN SCHEMA
- : Optional function to support TRUNCATE on foreign tables
- : Optional function to check if scanning is parallel-safe
- : Optional function to estimate dynamic shared memory for parallel scans
- : Optional function to initialize DSM for parallel scans
- : Optional function to reinitialize DSM for parallel scans
- : Optional function to initialize worker processes
- : Optional function to shut down parallel scanning
- : Optional function for path reparameterization
- : Optional function to check asynchronous execution capability
- : Optional function to request asynchronous execution
- : Optional function to configure waiting for async operations
- : Optional function to handle async operation notifications

## Dependencies
- Functions called/Symbols referenced:
  - ImportForeignSchema
- Called from (representative examples):
  - GetFdwRoutine
  - GetFdwRoutineByServerId
  - GetFdwRoutineByRelId
  - GetFdwRoutineForRelation
  - ExecInitForeignScan
  - make_modifytable
  - select_rowmark_type
  - analyze_rel
  - show_foreignscan_info
  - ExecuteTruncateGuts

## Notes and Other Information
The FdwRoutine structure is central to PostgreSQL's Foreign Data Wrapper architecture and serves as the contract between FDW implementations and the core database engine. FDW developers must implement at least the basic scanning functions, while other functions can be implemented as needed based on the capabilities of the foreign data source. The structure's design allows for future extensibility without breaking existing FDW implementations, as new function pointers can be added and existing FDWs will have them initialized to NULL by makeNode().