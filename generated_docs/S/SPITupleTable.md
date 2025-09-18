# SPITupleTable

## Location
src/include/executor/spi.h: 22 - 34

## Overview
SPITupleTable is a structure that represents a result table returned by SPI (Server Programming Interface) operations, containing an array of tuples along with their metadata and memory management information.

## Definition


## Detailed Description
SPITupleTable serves as the primary container for query results in PostgreSQL's Server Programming Interface. It encapsulates both the actual tuple data and the metadata necessary to interpret and manage that data. The structure is designed with a clear separation between public members (intended for external use) and private members (used internally by the SPI system for memory management and bookkeeping).

The structure supports efficient memory management through its dedicated memory context and tracks allocation information to enable dynamic resizing of the tuple array when needed. It also maintains transaction context information to ensure proper cleanup during subtransaction rollbacks.

## Parameters / Member Variables
### Public Members
- : TupleDesc containing the schema information for the tuples (column names, types, etc.)
- : Array of HeapTuple pointers containing the actual tuple data
- : Number of valid tuples currently stored in the vals array

### Private Members
- : Total allocated size of the vals array (may be larger than numvals for efficiency)
- : Memory context in which the tuple table and its data are allocated
- : Linked list node for internal SPI bookkeeping and cleanup tracking
- : Subtransaction ID in which this tuple table was created, used for proper cleanup

## Dependencies
- Functions called/Symbols referenced:
  - slist_node
  - SubTransactionId
  - TupleDesc
  - HeapTuple
  - MemoryContext

- Called from (representative examples):
  - AtEOSubXact_SPI
  - SPI_freetuptable
  - spi_dest_startup
  - spi_printtup
  - _SPI_execute_plan
  - _SPI_checktuples
  - plperl_spi_execute_fetch_result
  - PLy_spi_execute_fetch_result
  - pltcl_process_SPI_result

## Notes and Other Information
- The distinction between public and private members is crucial for API stability and proper encapsulation
- Memory management is handled automatically by the SPI system through the tuptabcxt memory context
- The structure supports both small and large result sets through its dynamic allocation strategy
- Proper cleanup during subtransaction rollbacks is ensured through the subid tracking
- External callers should only access the public members and use SPI_freetuptable() to release resources
- The structure is widely used across PostgreSQL's procedural language implementations (PL/Perl, PL/Python, PL/Tcl)