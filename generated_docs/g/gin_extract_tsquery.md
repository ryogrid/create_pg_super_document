# gin_extract_tsquery

## Location
[src/backend/utils/adt/tsginidx.c:94-176](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsginidx.c#L94-L176)

## Overview
Extracts searchable terms from a TSQuery (text search query) for GIN index lookups, determining search strategy and handling partial matches.

## Definition

```c
typedef struct
{
	QueryItem  *first_item;
	GinTernaryValue *check;
	int		   *map_item_operand;
} GinChkVal;
```
## Detailed Description
This function is a critical component of PostgreSQL's GIN indexing system for text search queries. It analyzes a TSQuery object and extracts individual searchable terms (lexemes) that need to be looked up in the GIN index. The function performs several key operations:

1. **Query Analysis**: Examines the query structure to determine if it contains required positive matches
2. **Search Mode Selection**: Sets appropriate search mode (DEFAULT or ALL) based on query requirements
3. **Term Extraction**: Identifies and extracts QI_VAL (value) items from the query tree
4. **Partial Match Handling**: Tracks which terms support prefix matching
5. **Mapping Creation**: Builds correspondence between query items and extracted operands

The function handles complex queries including negation, boolean operations, and prefix searches, ensuring the GIN index can efficiently locate relevant documents.

## Parameters / Member Variables
- Uses PostgreSQL's PG_FUNCTION_ARGS macro for function arguments:
  - Argument 0 (`query`): Input text search query to extract from
  - Argument 1 (`nentries`): Output pointer for number of extracted entries
  - Argument 2 (`strategy`): Search strategy (unused, commented out)
  - Argument 3 (`partial_matches`): Output array indicating partial match capability
  - Argument 4 (`extra_data`): Output array for additional per-entry data
  - Argument 5 (`nullFlags`): Null flags (unused, commented out)
  - Argument 6 (`searchMode`): Output search mode (DEFAULT or ALL)

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract TSQuery from function arguments
  -  - Extract pointer arguments
  -  - Get QueryItem array from TSQuery
  -  - Get operand string data from TSQuery
  -  - Check if query requires positive matches
  -  - Convert C string to PostgreSQL text
  -  - Convert pointer to Datum
  -  - Allocate memory
  -  - Allocate zero-initialized memory
  -  - Free TSQuery if copied
  -  - Return entries array
  -  - Text search query data type
  -  - Individual query tree node
  -  - [Query](../Q/Query.md) operand structure
  -  - [Query](../Q/Query.md) item type constant for values
  -  - Standard search mode
  -  - Full index scan mode
  -  - GIN ternary logic values
- Called from (representative examples):
  -  - Five-argument wrapper variant
  -  - Legacy signature wrapper

## Notes and Other Information
- Sets search mode to ALL for queries requiring full index scan (e.g., pure negation queries)
- Creates mapping between query items and extracted operands for consistent evaluation
- Handles prefix matching by examining QueryOperand prefix flags
- Memory allocation uses PostgreSQL's palloc family for proper memory management
- Essential for query planning and execution in GIN-indexed text search
- Supports complex boolean queries with AND, OR, NOT operations
- Part of PostgreSQL's full-text search operator class infrastructure

## Simplified Source

```c
Datum
gin_extract_tsquery(PG_FUNCTION_ARGS)
{
    TSQuery query = PG_GETARG_TSQUERY(0);
    int32 *nentries = (int32 *) PG_GETARG_POINTER(1);
    bool **ptr_partialmatch = (bool **) PG_GETARG_POINTER(3);
    Pointer **extra_data = (Pointer **) PG_GETARG_POINTER(4);
    int32 *searchMode = (int32 *) PG_GETARG_POINTER(6);
    Datum *entries = NULL;

    *nentries = 0;

    if (query->size > 0) {
        QueryItem *item = GETQUERY(query);

        // Determine search strategy based on query requirements
        if (tsquery_requires_match(item))
            *searchMode = GIN_SEARCH_MODE_DEFAULT;
        else
            *searchMode = GIN_SEARCH_MODE_ALL;  // Full scan for pure negation

        // Count VAL (lexeme) items in query
        int j = 0;
        for (int i = 0; i < query->size; i++) {
            if (item[i].type == QI_VAL)
                j++;
        }
        *nentries = j;

        if (j > 0) {
            // Allocate result arrays
            entries = palloc(sizeof(Datum) * j);
            bool *partialmatch = *ptr_partialmatch = palloc(sizeof(bool) * j);
            *extra_data = palloc(sizeof(Pointer) * j);
            int *map_item_operand = palloc0(sizeof(int) * query->size);

            // Extract searchable terms from query
            j = 0;
            for (int i = 0; i < query->size; i++) {
                if (item[i].type == QI_VAL) {
                    QueryOperand *val = &item[i].qoperand;

                    // Convert lexeme to text datum
                    text *txt = cstring_to_text_with_len(GETOPERAND(query) + val->distance,
                                                         val->length);
                    entries[j] = PointerGetDatum(txt);
                    partialmatch[j] = val->prefix;  // Track prefix search capability
                    (*extra_data)[j] = (Pointer) map_item_operand;
                    map_item_operand[i] = j;  // Map query position to operand
                    j++;
                }
            }
        }
    }

    PG_FREE_IF_COPY(query, 0);
    PG_RETURN_POINTER(entries);
}
```