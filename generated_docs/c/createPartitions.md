# createPartitions

## Location
src/bin/pgbench/pgbench.c: 4754 - 4822

## Overview
Creates partitions for the pgbench_accounts table to improve performance and manage large datasets by distributing data across multiple partition tables.

## Definition
```c
static void createPartitions(PGconn *con)
```

## Detailed Description
The createPartitions function creates partitioned tables for the pgbench_accounts table, which is the largest table in the pgbench TPC-B-like schema. It supports two partitioning methods: RANGE partitioning and HASH partitioning. For RANGE partitioning, it creates open-ended partitions at the beginning and end to accommodate any valid primary key values, calculating partition boundaries based on the total number of accounts and scale factor. For HASH partitioning, it distributes data using modulus and remainder values. Each partition is created with the same fillfactor setting as the parent table and can optionally be created as unlogged tables for better performance.

## Parameters / Member Variables
- `con`: PGconn pointer to the PostgreSQL database connection used to execute the partition creation statements

## Dependencies
- Functions called/Symbols referenced:
  - PQExpBufferData (for building SQL statements)
  - initPQExpBuffer (initializes query buffer)
  - printfPQExpBuffer (formats SQL statements)
  - appendPQExpBuffer (appends formatted text to buffer)
  - appendPQExpBufferStr (appends string to buffer)
  - appendPQExpBufferChar (appends character to buffer)
  - executeStatement (executes the SQL statements)
  - termPQExpBuffer (cleans up query buffer)
  - PART_RANGE, PART_HASH (partition method constants)
  - INT64_FORMAT (format macro for 64-bit integers)
- Called from (representative examples):
  - ddlinfo (as part of the table creation process)

## Notes and Other Information
- Only called when partitions > 0, enforced by Assert
- Supports both RANGE and HASH partitioning methods
- For RANGE partitioning, uses 'minvalue' and 'maxvalue' for boundary partitions
- Calculates partition size based on naccounts * scale / partitions
- Each partition inherits the fillfactor setting from the parent table
- Partition names follow the pattern pgbench_accounts_N where N is the partition number
- Can create unlogged partitions for better performance when unlogged_tables is enabled
- Uses Assert(0) for unreachable code path when partition_method is neither RANGE nor HASH