# tsq_mcontains

## Location
[src/backend/utils/adt/tsquery_op.c:307-353](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_op.c#L307-L353)

## Overview
Implements the PostgreSQL text search query containment operator, determining if the first TSQuery contains all the terms present in the second TSQuery.

## Definition
```c
Datum tsq_mcontains(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the @> (contains) operator for TSQuery objects. It extracts all textual values from both input queries, removes duplicates by sorting and using qunique, then performs a containment check to determine if the first query contains all terms present in the second query. The algorithm works by comparing sorted arrays of unique query terms, making it efficient for large queries with many repeated terms. The function returns true if every term in the second query exists in the first query, false otherwise.

## Parameters / Member Variables
- Function follows PostgreSQL's PG_FUNCTION_ARGS convention:
  - Argument 0: `query` - The TSQuery that should contain the terms
  - Argument 1: `ex` - The TSQuery whose terms should be contained

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TSQUERY (PostgreSQL macro to extract TSQuery arguments)
  - [collectTSQueryValues](../c/collectTSQueryValues.md) (extracts string values from TSQuery)
  - qsort (standard C sorting function)
  - [cmp_string](../c/cmp_string.md) (string comparison function for sorting)
  - [qunique](../q/qunique.md) (PostgreSQL utility to remove duplicates from sorted arrays)
  - strcmp (standard C string comparison)
  - PG_RETURN_BOOL (PostgreSQL macro to return boolean result)
- Types referenced:
  - TSQuery (text search query structure)
  - Datum (PostgreSQL generic data type)
- Called from (representative examples):
  - [tsq_mcontained](tsq_mcontained.md) (at src/backend/utils/adt/tsquery_op.c:356, implements the reverse containment operator)

## Notes and Other Information
- This is a PostgreSQL SQL function accessible as the @> operator for TSQuery types
- The algorithm has O(n log n + m log m) time complexity due to sorting, where n and m are the number of terms in each query
- Duplicate terms in queries are automatically eliminated, making the containment check more efficient
- The containment check uses an optimized algorithm that takes advantage of both arrays being sorted
- Short-circuits early if the second query has more unique terms than the first (impossible to contain)
- Memory allocation is handled by PostgreSQL's memory management system and automatically freed at function end

## Simplified Source

```c
Datum
tsq_mcontains(PG_FUNCTION_ARGS)
{
    TSQuery query = PG_GETARG_TSQUERY(0);
    TSQuery ex = PG_GETARG_TSQUERY(1);

    // Extract and sort unique terms from both queries
    char **query_values;
    int query_nvalues;
    char **ex_values;
    int ex_nvalues;

    query_values = collectTSQueryValues(query, &query_nvalues);
    ex_values = collectTSQueryValues(ex, &ex_nvalues);

    // Sort and remove duplicates
    qsort(query_values, query_nvalues, sizeof(char *), cmp_string);
    query_nvalues = qunique(query_values, query_nvalues, sizeof(char *), cmp_string);
    qsort(ex_values, ex_nvalues, sizeof(char *), cmp_string);
    ex_nvalues = qunique(ex_values, ex_nvalues, sizeof(char *), cmp_string);

    // Quick check: if ex has more terms than query, cannot contain
    if (ex_nvalues > query_nvalues)
        PG_RETURN_BOOL(false);

    // Check if query contains all terms from ex
    bool result = true;
    int j = 0;
    for (int i = 0; i < ex_nvalues; i++)
    {
        // Find ex_values[i] in query_values starting from j
        for (; j < query_nvalues; j++)
        {
            if (strcmp(ex_values[i], query_values[j]) == 0)
                break;
        }
        if (j == query_nvalues)  // Not found
        {
            result = false;
            break;
        }
    }

    PG_RETURN_BOOL(result);
}
```