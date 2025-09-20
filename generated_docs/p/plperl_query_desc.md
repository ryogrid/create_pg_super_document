# plperl_query_desc

## Location
[src/pl/plperl/plperl.c:186-195](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L186-L195)

## Overview
A structure that caches information about prepared and saved SQL plans in the PL/Perl procedural language extension for PostgreSQL.

## Definition

```c
typedef struct plperl_query_desc
{
	char		qname[24];
	MemoryContext plan_cxt;		/* context holding this struct */
	SPIPlanPtr	plan;
	int			nargs;
	Oid		   *argtypes;
	FmgrInfo   *arginfuncs;
	Oid		   *argtypioparams;
} plperl_query_desc;
```
## Detailed Description
The  structure serves as a cache for prepared SQL plans within PL/Perl functions. This structure is essential for the Server Programming Interface (SPI) functionality in PL/Perl, allowing for efficient reuse of prepared statements. The structure maintains all necessary metadata about a prepared query, including its execution plan, parameter information, and memory context management details.

## Parameters / Member Variables
- : Fixed-size character array storing the query name identifier (up to 23 characters plus null terminator)
- : Memory context that holds this structure and associated data, ensuring proper memory management
- : Pointer to the SPI plan structure containing the compiled query execution plan
- : Number of arguments/parameters that the prepared query expects
- : Array of OID values representing the data types of each query parameter
- : Array of function manager info structures for input functions used to convert parameter values
- : Array of OID values for type-specific I/O parameters used during argument conversion

## Dependencies
- Functions called/Symbols referenced:
  - [SPIPlanPtr](../S/SPIPlanPtr.md) (SPI plan pointer type)
- Called from (representative examples):
  - [plperl_query_entry](plperl_query_entry.md) (as a member structure)
  - [plperl_spi_prepare](plperl_spi_prepare.md)
  - [plperl_spi_exec_prepared](plperl_spi_exec_prepared.md)
  - [plperl_spi_query_prepared](plperl_spi_query_prepared.md)
  - [plperl_spi_freeplan](plperl_spi_freeplan.md)

## Notes and Other Information
- This structure is primarily used for caching prepared statements to avoid repeated parsing and planning overhead
- The fixed-size  field limits query names to 23 characters, which should be sufficient for most use cases
- Memory management is handled through the  field, ensuring that all associated data is properly cleaned up
- The structure integrates closely with PostgreSQL's SPI (Server Programming Interface) system for executing SQL from procedural languages