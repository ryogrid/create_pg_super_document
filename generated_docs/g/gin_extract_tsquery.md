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
- : Standard PostgreSQL function argument structure containing:
  - First argument (index 0):  - Input text search query to extract from
  - Second argument (index 1):  - Output pointer for number of extracted entries
  - Third argument (index 2):  - Search strategy (unused, commented out)
  - Fourth argument (index 3):  - Output array indicating partial match capability
  - Fifth argument (index 4):  - Output array for additional per-entry data
  - Sixth argument (index 5):  - Null flags (unused, commented out)
  - Seventh argument (index 6):  - Output search mode (DEFAULT or ALL)

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