# prepare_heap_command

## Location
[src/bin/pg_amcheck/pg_amcheck.c:841-880](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_amcheck/pg_amcheck.c#L841-L880)

## Overview
This function constructs a SQL command for running amcheck verification on a heap relation using PostgreSQL's verify_heapam() function.

## Definition

```c
static void
prepare_heap_command(PQExpBuffer sql, RelationInfo *rel, PGconn *conn)
```
## Detailed Description
The prepare_heap_command function creates a SQL query that invokes the verify_heapam() function from the amcheck extension to validate the integrity of a heap table. The function constructs a parameterized SQL command that:

1. **Builds a SELECT statement** that retrieves block number, offset number, attribute number, and error messages from verify_heapam()
2. **Configures verification parameters** including error handling behavior, toast table checking, and block range specification
3. **Filters out temporary tables** by adding a WHERE clause that excludes relations with persistence type 't' (temporary)
4. **Handles optional block range limits** by conditionally adding startblock and endblock parameters when specified

The generated SQL command follows a specific column order and naming convention expected by the verify_heap_slot_handler function that processes the results.

## Parameters / Member Variables
- `sql`: PQExpBuffer into which the constructed SQL command will be written
- `*rel`: RelationInfo structure containing information about the heap table to be checked, including relation OID and database info
- `*conn`: PGconn connection handle used for string escaping purposes (though not actively used in current implementation)
## Dependencies
- Functions called/Symbols referenced:
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md) (clears the SQL buffer)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md) (appends formatted text to buffer)
  - [RelationInfo](../R/RelationInfo.md) (structure type for relation metadata)
  - INT64_FORMAT (formatting macro for 64-bit integers)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_amcheck.c:774)

## Notes and Other Information
- The function is static, meaning it's only accessible within the pg_amcheck.c compilation unit
- Temporary tables are automatically skipped using the condition "c.relpersistence != 't'" to avoid unnecessary errors
- The function uses global opts structure to access configuration options like on_error_stop, reconcile_toast, skip pattern, and block range settings
- Block range parameters (startblock/endblock) are only added to the SQL when they have non-negative values
- Located in src/bin/pg_amcheck/pg_amcheck.c:841-880

## Simplified Source

```c
static void prepare_heap_command(PQExpBuffer sql, RelationInfo *rel, PGconn *conn) {
    // Clear the SQL buffer and start building the command
    resetPQExpBuffer(sql);

    // Build the main SELECT query calling verify_heapam()
    appendPQExpBuffer(sql,
        "SELECT v.blkno, v.offnum, v.attnum, v.msg "
        "FROM pg_catalog.pg_class c, %s.verify_heapam("
        "\nrelation := c.oid, on_error_stop := %s, check_toast := %s, skip := '%s'",
        rel->datinfo->amcheck_schema,
        opts.on_error_stop ? "true" : "false",
        opts.reconcile_toast ? "true" : "false",
        opts.skip);

    // Add optional block range parameters if specified
    if (opts.startblock >= 0) {
        appendPQExpBuffer(sql, ", startblock := " INT64_FORMAT, opts.startblock);
    }
    if (opts.endblock >= 0) {
        appendPQExpBuffer(sql, ", endblock := " INT64_FORMAT, opts.endblock);
    }

    // Complete the query with WHERE clause to filter target relation
    // and exclude temporary tables
    appendPQExpBuffer(sql,
        "\n) v WHERE c.oid = %u "
        "AND c.relpersistence != 't'",
        rel->reloid);
}
```