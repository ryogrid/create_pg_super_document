# cleanup_tsquery_stopwords

## Location
[src/backend/utils/adt/tsquery_cleanup.c:387-446](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_cleanup.c#L387-L446)

## Overview
Main entry point for removing stopword nodes from a TSQuery, converting between flat and tree representations while handling memory allocation and phrase distance adjustments.

## Definition

```c
TSQuery
cleanup_tsquery_stopwords(TSQuery in, bool noisy)
```
## Detailed Description
The `cleanup_tsquery_stopwords` function is the primary interface for removing stopwords from TSQuery structures in PostgreSQL's text search system. It orchestrates the complete cleanup process: converting the flat TSQuery representation to a tree, removing stopwords while adjusting phrase distances, and then converting back to the optimized flat representation.

The function performs several key operations:
1. **Tree conversion**: Uses `maketree` to convert the flat QueryItem array to a tree structure
2. **Stopword removal**: Calls `clean_stopword_intree` to recursively remove QI_VALSTOP nodes
3. **Distance adjustment**: Handles phrase operator distance corrections from the cleanup process
4. **Memory calculation**: Uses `calcstrlen` to determine required memory for the cleaned query
5. **Reconstruction**: Uses `plaintree` to convert back to flat representation and copies operand strings

If the entire query becomes empty after stopword removal, the function can optionally emit a notice and returns an empty TSQuery.

## Parameters / Member Variables
- `in`: Input TSQuery structure containing the original query with potential stopwords
- `noisy`: Boolean flag controlling whether to emit notices when queries become empty after cleanup

## Dependencies
- Functions called/Symbols referenced:
  - [maketree](../m/maketree.md): Converts flat TSQuery to tree structure
  - [clean_stopword_intree](clean_stopword_intree.md): Recursively removes stopwords from tree
  - [calcstrlen](calcstrlen.md): Calculates string storage requirements for cleaned tree
  - [plaintree](../p/plaintree.md): Converts cleaned tree back to flat QueryItem array
  - `GETQUERY`: Macro to access QueryItem array from TSQuery
  - `GETOPERAND`: Macro to access operand strings from TSQuery
  - `COMPUTESIZE`: Macro to calculate total TSQuery size
  - `SET_VARSIZE`: Macro to set variable-length object size
  - [palloc](../p/palloc.md): PostgreSQL memory allocation
  - `memcpy`: Memory copying for operand strings
  - `ereport`: Error/notice reporting system
  - Various type constants: `TSQuery`, `NODE`, `QueryItem`, `QueryOperand`, `QI_VAL`, `NOTICE`, `HDRSIZETQ`

- Called from (representative examples):
  - [parse_tsquery](../p/parse_tsquery.md): Main TSQuery parsing function in tsquery.c:936

## Notes and Other Information
- This is the main public interface for TSQuery stopword cleanup in PostgreSQL
- Handles complete memory management including allocation of the output TSQuery structure
- Preserves operand string data by copying from input to output with updated offsets
- The `noisy` parameter allows callers to control user-visible feedback about empty queries
- Returns a newly allocated TSQuery that must be managed by the caller
- Handles edge cases like completely empty queries after stopword removal
- The function maintains the original TSQuery format while internally using tree structures for processing
- Part of PostgreSQL's text search optimization pipeline, typically called during query parsing

## Simplified Source

```c
TSQuery cleanup_tsquery_stopwords(TSQuery in, bool noisy) {
    // Handle empty input
    if (in->size == 0)
        return in;

    // Convert to tree and remove stopwords
    int ladd, radd;
    NODE *root = clean_stopword_intree(maketree(GETQUERY(in)), &ladd, &radd);

    // Handle case where everything was removed
    if (root == NULL) {
        if (noisy) {
            ereport(NOTICE,
                (errmsg("text-search query contains only stop words or doesn't contain lexemes, ignored")));
        }
        TSQuery empty_query = palloc(HDRSIZETQ);
        empty_query->size = 0;
        SET_VARSIZE(empty_query, HDRSIZETQ);
        return empty_query;
    }

    // Calculate memory requirements and convert back to flat format
    int32 string_length = calcstrlen(root);
    int len;
    QueryItem *items = plaintree(root, &len);
    int32 total_size = COMPUTESIZE(len, string_length);

    // Allocate and initialize output TSQuery
    TSQuery result = palloc(total_size);
    SET_VARSIZE(result, total_size);
    result->size = len;

    // Copy query items and operand strings
    memcpy(GETQUERY(result), items, len * sizeof(QueryItem));

    QueryItem *result_items = GETQUERY(result);
    char *result_operands = GETOPERAND(result);

    // Copy operand strings and update offsets
    for (int i = 0; i < result->size; i++) {
        QueryOperand *op = (QueryOperand *) &result_items[i];

        if (op->type != QI_VAL)
            continue;

        memcpy(result_operands, GETOPERAND(in) + op->distance, op->length);
        result_operands[op->length] = '\0';
        op->distance = result_operands - GETOPERAND(result);
        result_operands += op->length + 1;
    }

    return result;
}
```