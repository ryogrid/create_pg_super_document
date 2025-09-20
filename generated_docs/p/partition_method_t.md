# partition_method_t

## Location
[src/bin/pgbench/pgbench.c:231-242](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L231-L242)

## Overview
The partition_method_t enum defines the partitioning strategies available for the pgbench_accounts table in the PostgreSQL benchmarking tool pgbench.

## Definition

```c
typedef enum
{
	PART_NONE,					/* no partitioning */
	PART_RANGE,					/* range partitioning */
	PART_HASH,					/* hash partitioning */
} partition_method_t;
```
## Detailed Description
This enumeration type specifies the partitioning method to be used for the pgbench_accounts table during benchmark initialization. It supports three distinct partitioning strategies:

- **PART_NONE**: Default behavior with no partitioning applied to the accounts table
- **PART_RANGE**: Implements range-based partitioning where data is distributed across partitions based on value ranges
- **PART_HASH**: Implements hash-based partitioning where data is distributed using hash functions for even distribution

The enum is used in conjunction with the global  variable and the  string array to control and represent partitioning behavior throughout the pgbench application.

## Parameters / Member Variables
- : Indicates no partitioning should be applied (default value)
- : Specifies range partitioning strategy 
- : Specifies hash partitioning strategy

## Dependencies
- Functions called/Symbols referenced:
  - PART_NONE (enum value)
- Called from (representative examples):
  - Used by partition_method static variable
  - Referenced in PARTITION_METHOD string array

## Notes and Other Information
- Located in src/bin/pgbench/pgbench.c at lines 226-231
- The corresponding string representations are maintained in the PARTITION_METHOD array: {"none", "range", "hash"}
- Default partitioning method is PART_NONE as initialized in the static partition_method variable
- This enum is part of pgbench's table initialization and benchmarking configuration system