# pg_prepared_statement

## Location
[src/backend/commands/prepare.c:684-745](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/prepare.c#L684-L745)

## Overview
A set-returning function that reads all prepared statements and returns detailed metadata about each statement including name, query text, preparation time, parameter types, and plan statistics.

## Definition
```c
Datum pg_prepared_statement(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements a system view that provides introspection capabilities for prepared statements in the current session. It scans the prepared_queries hash table and returns a comprehensive set of information for each prepared statement:

1. **Statement Identification**: Statement name and original query text
2. **Timing Information**: When the statement was prepared
3. **Type Information**: Parameter types and result column types (if applicable)
4. **Execution Statistics**: Count of generic vs custom plans used
5. **Source Tracking**: Whether the statement was created from SQL

The function uses PostgreSQL's set-returning function infrastructure with materialized results, putting all tuples into a tuplestore in a single hash table scan to avoid concurrency issues. For statements without result descriptors (like DML statements), the result types field is set to NULL.

## Parameters / Member Variables
This function follows PostgreSQL's SRF (Set-Returning Function) convention:
- Takes `PG_FUNCTION_ARGS` which provides access to function call context
- Returns `Datum` (0 for SRFs)
- Uses `fcinfo->resultinfo` to access the ReturnSetInfo structure

## Dependencies
- Functions called/Symbols referenced:
  - [InitMaterializedSRF](../I/InitMaterializedSRF.md)
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - CStringGetTextDatum
  - [TimestampTzGetDatum](../T/TimestampTzGetDatum.md)
  - [build_regtype_array](../b/build_regtype_array.md)
  - palloc_array
  - [BoolGetDatum](../B/BoolGetDatum.md)
  - Int64GetDatumFast
  - [tuplestore_putvalues](../t/tuplestore_putvalues.md)
- Data structures used:
  - [ReturnSetInfo](../R/ReturnSetInfo.md)
  - [HASH_SEQ_STATUS](../H/HASH_SEQ_STATUS.md)
  - [PreparedStatement](../P/PreparedStatement.md)
  - [TupleDesc](../T/TupleDesc.md)
  - prepared_queries (global hash table)
- Called from (representative examples):
  - System view queries (via SQL interface)

## Notes and Other Information
- Returns 8 columns: name, statement, prepare_time, param_types, result_types, from_sql, generic_plans, custom_plans
- Safely handles the case where no prepared statements exist (prepared_queries is NULL)
- Uses materialized SRF approach to avoid hash table changes during iteration
- Parameter and result types are returned as regtype arrays using build_regtype_array
- The result_types column is NULL for statements without result descriptors (e.g., INSERT, UPDATE, DELETE)
- [Plan](../P/Plan.md) statistics (generic_plans, custom_plans) provide insight into PostgreSQL's adaptive planning behavior
- The from_sql flag distinguishes between statements prepared via SQL PREPARE vs. protocol-level preparation
- This function is typically exposed through the pg_prepared_statements system view

## Simplified Source

```c
Datum
pg_prepared_statement(PG_FUNCTION_ARGS)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

    // Initialize materialized set-returning function
    InitMaterializedSRF(fcinfo, 0);

    // Scan prepared statements hash table if it exists
    if (prepared_queries)
    {
        HASH_SEQ_STATUS hash_seq;
        PreparedStatement *prep_stmt;

        hash_seq_init(&hash_seq, prepared_queries);
        while ((prep_stmt = hash_seq_search(&hash_seq)) != NULL)
        {
            TupleDesc result_desc;
            Datum values[8];
            bool nulls[8] = {0};

            result_desc = prep_stmt->plansource->resultDesc;

            // Fill result columns: name, statement, prepare_time, param_types
            values[0] = CStringGetTextDatum(prep_stmt->stmt_name);
            values[1] = CStringGetTextDatum(prep_stmt->plansource->query_string);
            values[2] = TimestampTzGetDatum(prep_stmt->prepare_time);
            values[3] = build_regtype_array(prep_stmt->plansource->param_types,
                                          prep_stmt->plansource->num_params);

            // Handle result types (nullable for DML statements)
            if (result_desc)
            {
                Oid *result_types;

                result_types = palloc_array(Oid, result_desc->natts);
                for (int i = 0; i < result_desc->natts; i++)
                    result_types[i] = result_desc->attrs[i].atttypid;
                values[4] = build_regtype_array(result_types, result_desc->natts);
            }
            else
            {
                nulls[4] = true;
            }

            // Fill remaining columns: from_sql, generic_plans, custom_plans
            values[5] = BoolGetDatum(prep_stmt->from_sql);
            values[6] = Int64GetDatumFast(prep_stmt->plansource->num_generic_plans);
            values[7] = Int64GetDatumFast(prep_stmt->plansource->num_custom_plans);

            tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
        }
    }

    return (Datum) 0;
}
```