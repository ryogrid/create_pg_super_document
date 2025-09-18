# ExecOpenIndices

## Location
src/backend/executor/execIndexing.c: 156 - 230

## Overview
Opens all indices associated with a result relation and stores their descriptors and metadata in the ResultRelInfo structure for subsequent index operations during tuple modification.

## Definition


## Detailed Description
The ExecOpenIndices function is a critical component of PostgreSQL's executor that prepares index relations for modification operations (INSERT, UPDATE, DELETE). It discovers all indices associated with a given result relation, opens each index with appropriate locking, and caches the index descriptors and metadata in the ResultRelInfo structure.

The function performs several key operations:
1. Checks if the relation has any indices using the relhasindex flag for fast-path optimization
2. Retrieves the list of index OIDs associated with the relation from the system catalog cache
3. Allocates memory for storing index relation descriptors and IndexInfo structures
4. For each index, opens the index relation with RowExclusiveLock (indicating intent to modify)
5. Builds IndexInfo metadata structures containing key information from pg_index
6. For speculative insertions on unique indices, builds additional speculative insertion metadata

The function handles both regular and speculative insertion scenarios. Speculative insertion is used for INSERT ... ON CONFLICT operations where PostgreSQL needs to detect constraint violations before committing the insertion.

## Parameters / Member Variables
- : Pointer to ResultRelInfo structure that will store the opened index information. The caller must have already opened and locked the main relation (ri_RelationDesc).
- : Boolean flag indicating whether the indices will be used for speculative insertion operations, which requires additional metadata for unique constraint handling.

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetForm: Accesses relation tuple form to check relhasindex flag
  - [RelationGetIndexList](../R/RelationGetIndexList.md): Retrieves cached list of index OIDs for the relation
  - [index_open](../i/index_open.md): Opens individual index relations with RowExclusiveLock
  - [BuildIndexInfo](../B/BuildIndexInfo.md): Extracts and builds index metadata from pg_index system catalog
  - [BuildSpeculativeIndexInfo](../B/BuildSpeculativeIndexInfo.md): Adds speculative insertion metadata for unique indices
  - [list_free](../l/list_free.md): Deallocates the temporary index OID list
- Called from (representative examples):
  - [ExecInsert](ExecInsert.md): Opens indices before inserting new tuples
  - [ExecUpdatePrologue](ExecUpdatePrologue.md): Opens indices before updating existing tuples
  - [CopyFrom](../C/CopyFrom.md): Opens indices for bulk data loading operations
  - [apply_handle_insert](../a/apply_handle_insert.md)/update/delete: Logical replication worker operations

## Notes and Other Information
- The function assumes the caller has already acquired appropriate locks on the main relation
- All indices are opened regardless of their indisready status - the optimization for non-ready indices is not implemented
- Memory allocation uses palloc, so the allocated structures persist for the duration of the current memory context
- The function sets ri_NumIndices to 0 initially and only updates it after successful allocation
- RowExclusiveLock is acquired on all indices, indicating intention to perform modifications
- For speculative operations, additional metadata is built only for unique indices since they're the only ones that can participate in conflict detection