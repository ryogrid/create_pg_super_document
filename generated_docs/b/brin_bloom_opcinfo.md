# brin_bloom_opcinfo

## Location
[src/backend/access/brin/brin_bloom.c:449-495](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_bloom.c#L449-L495)

## Overview
Initializes and returns operator class information structure for BRIN bloom indexes, configuring storage parameters and type information.

## Definition


## Detailed Description
This function serves as the opcinfo support function for BRIN bloom operator classes. It allocates and initializes a BrinOpcInfo structure that contains metadata about how the bloom operator class stores and manages summary information. The function sets up a single-column storage model (since bloom filters are stored as a single BYTEA column), configures null handling, and establishes the type cache for the bloom summary data type.

The function uses PostgreSQL's MAXALIGN macro to ensure proper memory alignment for the allocated structures, which is critical for performance and correctness on various architectures.

## Parameters / Member Variables
- Function follows PostgreSQL's PG_FUNCTION_ARGS convention (no explicit parameters, uses PostgreSQL's function call protocol)

## Dependencies
- Functions called/Symbols referenced:
  - [BrinOpcInfo](../B/BrinOpcInfo.md): BRIN operator class information structure
  - [BloomOpaque](../B/BloomOpaque.md): Bloom-specific opaque data structure  
  - SizeofBrinOpcInfo: Macro to calculate size of BrinOpcInfo structure
  - [palloc0](../p/palloc0.md): PostgreSQL memory allocation function (zero-initialized)
  - MAXALIGN: Memory alignment macro
  - [lookup_type_cache](../l/lookup_type_cache.md): Function to get type cache information
  - PG_BRIN_BLOOM_SUMMARYOID: OID constant for bloom summary data type
  - PG_RETURN_POINTER: PostgreSQL macro to return pointer values
- Called from (representative examples):
  - No direct references found (likely called by PostgreSQL's operator class system)

## Notes and Other Information
- This is a PostgreSQL SQL-callable function (returns Datum)
- Configures bloom indexes to store data as a single BYTEA column
- Sets oi_regular_nulls to true, indicating standard null value handling
- The strategy_procinfos array is initialized lazily as mentioned in comments
- Memory layout is carefully managed with MAXALIGN for cross-platform compatibility
- Part of PostgreSQL's BRIN (Block Range Index) extensibility framework
- Located in src/backend/access/brin/brin_bloom.c:449-495