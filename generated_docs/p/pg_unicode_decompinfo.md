# pg_unicode_decompinfo

## Location
src/include/common/unicode_norm_hashfunc.h: 31 - 37

## Overview
A data structure that encapsulates Unicode character decomposition information along with the associated hash function for efficient lookups in PostgreSQL's Unicode normalization system.

## Definition
```c
typedef struct
{
	const pg_unicode_decomposition *decomps;
	cp_hash_func	hash;
	int		num_decomps;
} pg_unicode_decompinfo;
```

## Detailed Description
`pg_unicode_decompinfo` is a structure that serves as a complete lookup package for Unicode character decomposition operations. It combines three essential components needed for efficient Unicode normalization: the actual decomposition data table, the perfect hash function for fast lookups, and metadata about the size of the decomposition table.

This structure is used to create a self-contained decomposition lookup system where the hash function can quickly map Unicode characters to their corresponding entries in the decomposition table. The design allows for O(1) average-case lookup performance, which is crucial for text processing and Unicode normalization operations.

An instance of this structure, `UnicodeDecompInfo`, is statically defined and contains references to the main Unicode decomposition table (`UnicodeDecompMain`) and the associated perfect hash function (`Decomp_hash_func`).

## Parameters / Member Variables
- `decomps`: Pointer to an array of `pg_unicode_decomposition` structures containing the actual Unicode decomposition data for characters that can be decomposed
- `hash`: Function pointer to a perfect hash function (specifically `Decomp_hash_func`) used to quickly locate entries in the decomposition table
- `num_decomps`: Integer specifying the total number of entries in the decomposition table, used for bounds checking and table management

## Dependencies
- Functions called/Symbols referenced:
  - pg_unicode_decomposition (struct type for individual decomposition entries)
  - cp_hash_func (typedef for hash function pointer)
  - Decomp_hash_func (the actual hash function used)
  - UnicodeDecompMain (the main decomposition table)
- Called from (representative examples):
  - get_code_entry (in unicode_norm.c for character decomposition lookups)
  - Used throughout PostgreSQL's Unicode normalization routines

## Notes and Other Information
- This structure provides a complete abstraction for Unicode decomposition lookups, encapsulating both data and the method to access it efficiently
- The design pattern allows for easy extensibility - different hash functions or decomposition tables could be used by creating different instances of this structure
- The structure is typically used as a static constant, providing read-only access to decomposition information
- The combination of perfect hashing and structured data access makes this an efficient solution for Unicode text processing
- This is part of PostgreSQL's internal Unicode handling infrastructure and is not exposed directly to user applications
- The structure design follows the principle of keeping related data and functionality together, making the code more maintainable and efficient