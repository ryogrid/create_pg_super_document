# libpqrcv_processTuples

## Location
src/backend/replication/libpqwalreceiver/libpqwalreceiver.c: 1159 - 1234

## Overview
libpqrcv_processTuples converts a PGresult containing tuple data from a query execution into a PostgreSQL tuplestore, handling proper type conversion and memory management for integration with the executor framework.

## Definition
```c
static void libpqrcv_processTuples(PGresult *pgres, WalRcvExecResult *walres,
                                   const int nRetTypes, const Oid *retTypes)
```

## Detailed Description
This function serves as a bridge between libpq query results and PostgreSQL's internal tuple storage system. It validates that the number of returned fields matches expectations, creates a tuplestore for efficient tuple storage, and builds a corresponding tuple descriptor based on the expected return types. The function processes each row from the PGresult by converting column values from C strings to appropriate PostgreSQL internal formats using BuildTupleFromCStrings. It employs careful memory management with a temporary context for row processing to avoid memory leaks during conversion. The resulting tuples are stored in a tuplestore that can be consumed by PostgreSQL's executor framework. This is essential for functions that need to return query results as tuples within the PostgreSQL system.

## Parameters / Member Variables
- `pgres`: PGresult containing the query results from libpq with tuple data
- `walres`: Output WalRcvExecResult structure that will contain the tuplestore and tuple descriptor
- `nRetTypes`: Expected number of return columns/fields in the result
- `retTypes`: Array of PostgreSQL type OIDs specifying the expected data types for each column

## Dependencies
- Functions called/Symbols referenced:
  - [PQnfields](../P/PQnfields.md)/PQntuples: libpq functions to get result dimensions
  - [PQfname](../P/PQfname.md): libpq function to get column names from results
  - [PQgetisnull](../P/PQgetisnull.md)/PQgetvalue: libpq functions to extract field values and NULL status
  - tuplestore_begin_heap: PostgreSQL function to create a new tuplestore
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md): PostgreSQL function to create tuple descriptor template
  - [TupleDescInitEntry](../T/TupleDescInitEntry.md): PostgreSQL function to initialize tuple descriptor attributes
  - [TupleDescGetAttInMetadata](../T/TupleDescGetAttInMetadata.md): PostgreSQL function to build metadata for tuple conversion
  - [BuildTupleFromCStrings](../B/BuildTupleFromCStrings.md): PostgreSQL function to construct tuples from C string arrays
  - tuplestore_puttuple: PostgreSQL function to add tuples to tuplestore
  - AllocSetContextCreate/MemoryContextSwitchTo: PostgreSQL memory management functions
  - [ProcessWalRcvInterrupts](../P/ProcessWalRcvInterrupts.md): WAL receiver interrupt processing function
- Called from (representative examples):
  - [libpqrcv_exec](libpqrcv_exec.md): Main query execution function that processes different result types

## Notes and Other Information
- This is a static function, accessible only within libpqwalreceiver.c
- Validates field count before processing to ensure protocol compliance
- Uses temporary memory contexts to prevent memory leaks during tuple conversion
- Handles NULL values properly by checking PQgetisnull before accessing field data
- The tuplestore is created with heap storage (true) and random access disabled (false)
- Memory usage is controlled by work_mem parameter for the tuplestore
- Processes interrupts during row iteration to maintain responsiveness
- The function assumes C string representation for all input data types
- Essential for functions that return tabular data through the WAL receiver interface
- Location: src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:1159-1234