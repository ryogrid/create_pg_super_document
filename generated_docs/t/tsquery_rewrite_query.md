# tsquery_rewrite_query

## Location
[src/backend/utils/adt/tsquery_rewrite.c:280-409](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_rewrite.c#L280-L409)

## Overview
The `tsquery_rewrite_query` function is a PostgreSQL SQL function that applies multiple rewrite rules to a TSQuery by executing a SQL query that returns replacement patterns.

## Definition
```c
Datum tsquery_rewrite_query(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the `ts_rewrite(tsquery, text)` SQL function, which allows users to rewrite TSQuery expressions by providing a SQL query that returns rewrite rules. The function executes the provided SQL query, which must return exactly two tsquery columns representing pattern-replacement pairs, and applies each rule sequentially to transform the input query.

The function works by:
1. Converting the input TSQuery to an internal tree representation
2. Executing the provided SQL query using the Server Programming Interface (SPI)
3. Processing each row from the query result as a rewrite rule (pattern → replacement)
4. Applying each rule using the `findsubquery` mechanism
5. Preparing the tree for subsequent rules by clearing processing flags and re-sorting
6. Converting the final tree back to TSQuery format

The function handles edge cases such as empty queries, invalid SQL queries, and queries that return incorrect column types. It uses proper memory management and SPI resource cleanup to ensure robust operation.

## Parameters / Member Variables
- `query`: Input TSQuery to be rewritten (argument 0)
- `in`: SQL query text that returns rewrite rules (argument 1)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TSQUERY_COPY (get TSQuery argument)
  - [QT2QTN](../Q/QT2QTN.md) (convert TSQuery to tree representation)
  - [QTNTernary](../Q/QTNTernary.md), QTNSort (tree preprocessing)
  - [SPI_connect](../S/SPI_connect.md), SPI_prepare, SPI_cursor_open (SQL execution)
  - [SPI_cursor_fetch](../S/SPI_cursor_fetch.md), SPI_getbinval (result processing)
  - [findsubquery](../f/findsubquery.md) (pattern replacement)
  - [QTNClearFlags](../Q/QTNClearFlags.md), QTNBinary (tree postprocessing)
  - [QTN2QT](../Q/QTN2QT.md) (convert tree back to TSQuery)
- Called from (representative examples):
  - SQL queries using ts_rewrite(tsquery, text) function

## Notes and Other Information
- Implements the two-argument version of ts_rewrite() SQL function
- Requires the SQL query to return exactly two tsquery columns (pattern and replacement)
- Processes rewrite rules in batches of 100 rows for memory efficiency
- Applies rules iteratively, allowing complex multi-step transformations
- Handles memory management carefully with proper SPI resource cleanup
- Returns an empty TSQuery if all patterns are eliminated during rewriting
- The function validates query result structure and provides appropriate error messages
- Uses cursor-based result processing to handle large result sets efficiently

## Simplified Source

```c
Datum
tsquery_rewrite_query(PG_FUNCTION_ARGS)
{
    TSQuery query = PG_GETARG_TSQUERY_COPY(0);
    text *sql_text = PG_GETARG_TEXT_PP(1);

    // Return empty queries unchanged
    if (query->size == 0)
    {
        PG_FREE_IF_COPY(sql_text, 1);
        PG_RETURN_POINTER(query);
    }

    // Convert to tree and prepare for matching
    QTNode *tree = QT2QTN(GETQUERY(query), GETOPERAND(query));
    QTNTernary(tree);
    QTNSort(tree);

    // Execute SQL query to get rewrite rules
    char *sql = text_to_cstring(sql_text);
    SPI_connect();

    SPIPlanPtr plan = SPI_prepare(sql, 0, NULL);
    if (!plan) elog(ERROR, "SPI_prepare failed for query: %s", sql);

    Portal portal = SPI_cursor_open(NULL, plan, NULL, NULL, true);
    if (!portal) elog(ERROR, "SPI_cursor_open failed for query: %s", sql);

    // Process rewrite rules in batches
    SPI_cursor_fetch(portal, true, 100);

    // Validate result structure
    if (SPI_tuptable &&
        (SPI_tuptable->tupdesc->natts != 2 ||
         SPI_gettypeid(SPI_tuptable->tupdesc, 1) != TSQUERYOID ||
         SPI_gettypeid(SPI_tuptable->tupdesc, 2) != TSQUERYOID))
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("query must return two tsquery columns")));
    }

    // Apply each rewrite rule
    while (SPI_processed > 0 && tree)
    {
        for (uint64 i = 0; i < SPI_processed && tree; i++)
        {
            bool isnull;
            Datum pattern_datum = SPI_getbinval(SPI_tuptable->vals[i],
                                               SPI_tuptable->tupdesc, 1, &isnull);
            if (isnull) continue;

            Datum replacement_datum = SPI_getbinval(SPI_tuptable->vals[i],
                                                   SPI_tuptable->tupdesc, 2, &isnull);

            TSQuery pattern = DatumGetTSQuery(pattern_datum);
            if (pattern->size == 0) continue;

            // Convert pattern to tree
            QTNode *pattern_tree = QT2QTN(GETQUERY(pattern), GETOPERAND(pattern));
            QTNTernary(pattern_tree);
            QTNSort(pattern_tree);

            // Handle replacement
            QTNode *replacement_tree = NULL;
            if (!isnull)
            {
                TSQuery replacement = DatumGetTSQuery(replacement_datum);
                if (replacement->size > 0)
                    replacement_tree = QT2QTN(GETQUERY(replacement), GETOPERAND(replacement));
            }

            // Apply the rewrite rule
            tree = findsubquery(tree, pattern_tree, replacement_tree, NULL);

            // Prepare for next iteration
            if (tree)
            {
                QTNClearFlags(tree, QTN_NOCHANGE);
                QTNTernary(tree);
                QTNSort(tree);
            }

            QTNFree(pattern_tree);
            QTNFree(replacement_tree);
        }

        SPI_freetuptable(SPI_tuptable);
        SPI_cursor_fetch(portal, true, 100);
    }

    // Cleanup SPI resources
    SPI_freetuptable(SPI_tuptable);
    SPI_cursor_close(portal);
    SPI_freeplan(plan);
    SPI_finish();

    // Convert result back to TSQuery
    TSQuery result;
    if (tree)
    {
        QTNBinary(tree);
        result = QTN2QT(tree);
        QTNFree(tree);
        PG_FREE_IF_COPY(query, 0);
    }
    else
    {
        // Empty result
        SET_VARSIZE(query, HDRSIZETQ);
        query->size = 0;
        result = query;
    }

    pfree(sql);
    PG_FREE_IF_COPY(sql_text, 1);
    PG_RETURN_POINTER(result);
}
```