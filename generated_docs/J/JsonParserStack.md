# JsonParserStack

## Location
[src/common/jsonapi.c:84-101](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/jsonapi.c#L84-L101)

## Overview
JsonParserStack is a structure containing the three stacks used in non-recursive JSON parsing, along with token and value storage for scalars that need to be preserved across parsing calls.

## Definition

```c
struct JsonParserStack
{
	int			stack_size;
	char	   *prediction;
	size_t		pred_index;
	/* these two are indexed by lex_level */
	char	  **fnames;
	bool	   *fnull;
	JsonTokenType scalar_tok;
	char	   *scalar_val;
};
```
## Detailed Description
JsonParserStack is a core data structure used in PostgreSQL's non-recursive JSON parsing implementation. It maintains the parsing state across multiple calls to incremental parsing functions, enabling the parser to handle large JSON documents without deep recursion that could cause stack overflow. The structure contains three main stacks: prediction stack for tracking parsing expectations, field name stack, and null indicator stack, along with storage for scalar tokens and values that span parsing boundaries.

## Parameters / Member Variables
- `stack_size`: The allocated size of the various stacks within the structure
- `*prediction`: Stack used for tracking parsing predictions and expectations during non-recursive parsing
- `pred_index`: Current index position within the prediction stack
- `**fnames`: Array of field name strings indexed by lexical level, used to track nested object field names
- `*fnull`: Array of boolean flags indexed by lexical level, indicating null values for corresponding field names
- `scalar_tok`: Token type for scalar values that need to be preserved across parsing calls
- `*scalar_val`: String value for scalars that need to be preserved across parsing calls
## Dependencies
- Functions called/Symbols referenced:
  - [JsonTokenType](JsonTokenType.md)
- Called from (representative examples):
  - makeJsonLexContextIncremental
  - [push_prediction](../p/push_prediction.md)
  - [pop_prediction](../p/pop_prediction.md)
  - [next_prediction](../n/next_prediction.md)
  - [have_prediction](../h/have_prediction.md)
  - [pg_parse_json](../p/pg_parse_json.md)
  - [pg_parse_json_incremental](../p/pg_parse_json_incremental.md)

## Notes and Other Information
The typedef for this structure appears in jsonapi.h, making it available throughout the PostgreSQL codebase. This structure is essential for incremental JSON parsing, allowing the parser to maintain state between calls and handle arbitrarily large JSON documents without risking stack overflow from deep recursion.