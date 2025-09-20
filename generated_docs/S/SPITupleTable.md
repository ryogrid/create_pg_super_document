# SPITupleTable

## Location
[src/include/executor/spi.h:22-34](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/spi.h#L22-L34)

## Overview
SPITupleTable is a structure that represents a result table returned by SPI (Server Programming Interface) operations, containing an array of tuples along with their metadata and memory management information.

## Definition

```c
typedef struct SPITupleTable
{
	/* Public members */
	TupleDesc	tupdesc;		/* tuple descriptor */
	HeapTuple  *vals;			/* array of tuples */
	uint64		numvals;		/* number of valid tuples */

	/* Private members, not intended for external callers */
	uint64		alloced;		/* allocated length of vals array */
	MemoryContext tuptabcxt;	/* memory context of result table */
	slist_node	next;			/* link for internal bookkeeping */
	SubTransactionId subid;		/* subxact in which tuptable was created */
} SPITupleTable;
```
## Detailed Description
SPITupleTable serves as the primary container for query results in PostgreSQL's Server Programming Interface. It encapsulates both the actual tuple data and the metadata necessary to interpret and manage that data. The structure is designed with a clear separation between public members (intended for external use) and private members (used internally by the SPI system for memory management and bookkeeping).

The structure supports efficient memory management through its dedicated memory context and tracks allocation information to enable dynamic resizing of the tuple array when needed. It also maintains transaction context information to ensure proper cleanup during subtransaction rollbacks.

## Parameters / Member Variables
- `tupdesc`: TupleDesc containing the schema information for the tuples (column names, types, etc.)
- `*vals`: Array of HeapTuple pointers containing the actual tuple data
- `numvals`: Number of valid tuples currently stored in the vals array
- `alloced`: Total allocated size of the vals array (may be larger than numvals for efficiency)
- `tuptabcxt`: Memory context in which the tuple table and its data are allocated
- `next`: Linked list node for internal SPI bookkeeping and cleanup tracking
- `subid`: Subtransaction ID in which this tuple table was created, used for proper cleanup
## Dependencies
- Functions called/Symbols referenced:
  - [slist_node](../s/slist_node.md)
  - SubTransactionId
  - [TupleDesc](../T/TupleDesc.md)
  - HeapTuple
  - [MemoryContext](../M/MemoryContext.md)

- Called from (representative examples):
  - [AtEOSubXact_SPI](../A/AtEOSubXact_SPI.md)
  - [SPI_freetuptable](SPI_freetuptable.md)
  - [spi_dest_startup](../s/spi_dest_startup.md)
  - [spi_printtup](../s/spi_printtup.md)
  - [_SPI_execute_plan](_SPI_execute_plan.md)
  - _SPI_checktuples
  - [plperl_spi_execute_fetch_result](../p/plperl_spi_execute_fetch_result.md)
  - [PLy_spi_execute_fetch_result](../P/PLy_spi_execute_fetch_result.md)
  - pltcl_process_SPI_result

## Notes and Other Information
- The distinction between public and private members is crucial for API stability and proper encapsulation
- Memory management is handled automatically by the SPI system through the tuptabcxt memory context
- The structure supports both small and large result sets through its dynamic allocation strategy
- Proper cleanup during subtransaction rollbacks is ensured through the subid tracking
- External callers should only access the public members and use SPI_freetuptable() to release resources
- The structure is widely used across PostgreSQL's procedural language implementations (PL/Perl, PL/Python, PL/Tcl)