# createPartitions

## Location
[src/bin/pgbench/pgbench.c:4754-4822](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L4754-L4822)

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
  - [PQExpBufferData](../P/PQExpBufferData.md) (for building SQL statements)
  - [initPQExpBuffer](../i/initPQExpBuffer.md) (initializes query buffer)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md) (formats SQL statements)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md) (appends formatted text to buffer)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md) (appends string to buffer)
  - [appendPQExpBufferChar](../a/appendPQExpBufferChar.md) (appends character to buffer)
  - [executeStatement](../e/executeStatement.md) (executes the SQL statements)
  - [termPQExpBuffer](../t/termPQExpBuffer.md) (cleans up query buffer)
  - PART_RANGE, PART_HASH (partition method constants)
  - INT64_FORMAT (format macro for 64-bit integers)
- Called from (representative examples):
  - [ddlinfo](../d/ddlinfo.md) (as part of the table creation process)

## Notes and Other Information
- Only called when partitions > 0, enforced by Assert
- Supports both RANGE and HASH partitioning methods
- For RANGE partitioning, uses 'minvalue' and 'maxvalue' for boundary partitions
- Calculates partition size based on naccounts * scale / partitions
- Each partition inherits the fillfactor setting from the parent table
- Partition names follow the pattern pgbench_accounts_N where N is the partition number
- Can create unlogged partitions for better performance when unlogged_tables is enabled
- Uses Assert(0) for unreachable code path when partition_method is neither RANGE nor HASH

## Simplified Source

```c
static void createPartitions(PGconn *con) {
    PQExpBufferData query;

    // Validate we have partitions to create
    Assert(partitions > 0);

    fprintf(stderr, "creating %d partitions...\n", partitions);
    initPQExpBuffer(&query);

    // Create each partition
    for (int p = 1; p <= partitions; p++) {
        if (partition_method == PART_RANGE) {
            // Calculate partition size for range partitioning
            int64 part_size = (naccounts * (int64) scale + partitions - 1) / partitions;

            // Build range partition SQL
            printfPQExpBuffer(&query,
                             "create%s table pgbench_accounts_%d\n"
                             "  partition of pgbench_accounts\n"
                             "  for values from (",
                             unlogged_tables ? " unlogged" : "", p);

            // Set range boundaries (minvalue for first, maxvalue for last)
            if (p == 1)
                appendPQExpBufferStr(&query, "minvalue");
            else
                appendPQExpBuffer(&query, INT64_FORMAT, (p - 1) * part_size + 1);

            appendPQExpBufferStr(&query, ") to (");

            if (p < partitions)
                appendPQExpBuffer(&query, INT64_FORMAT, p * part_size + 1);
            else
                appendPQExpBufferStr(&query, "maxvalue");

            appendPQExpBufferChar(&query, ')');
        }
        else if (partition_method == PART_HASH) {
            // Build hash partition SQL
            printfPQExpBuffer(&query,
                             "create%s table pgbench_accounts_%d\n"
                             "  partition of pgbench_accounts\n"
                             "  for values with (modulus %d, remainder %d)",
                             unlogged_tables ? " unlogged" : "", p,
                             partitions, p - 1);
        }

        // Add fillfactor and execute
        appendPQExpBuffer(&query, " with (fillfactor=%d)", fillfactor);
        executeStatement(con, query.data);
    }

    termPQExpBuffer(&query);
}
```