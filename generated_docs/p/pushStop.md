# pushStop

## Location
[src/backend/utils/adt/tsquery.c:616-626](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery.c#L616-L626)

## Overview
Creates and pushes a stopword placeholder onto the parser state's polish notation stack to represent words that should be ignored during text search operations.

## Definition
```c
void
pushStop(TSQueryParserState state)
```

## Detailed Description
This function creates a special type of QueryOperand that represents a stopword placeholder in the tsquery parsing process. Stopwords are common words (like "the", "and", "is") that are typically filtered out of text search operations because they don't contribute meaningful search criteria. The function allocates a minimal QueryOperand structure with only the type field set to QI_VALSTOP, indicating this is a stopword placeholder rather than a searchable term.

Unlike regular operands, stopword placeholders don't contain actual text content, weights, or other search-related metadata. They serve as markers in the query structure that can be optimized away or handled specially during query execution. This approach allows the parser to maintain the logical structure of the original query while marking certain positions as non-searchable.

## Parameters / Member Variables
- `state`: TSQueryParserState containing the current parsing context and polish notation stack

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](palloc0.md) (PostgreSQL memory allocation function)
  - [lcons](../l/lcons.md) (list construction function)
  - QueryOperand (query operand structure type)
  - QI_VALSTOP (query item type constant for stopword placeholders)
- Called from (representative examples):
  - [pushval_morph](pushval_morph.md)
  - [gettoken_query_websearch](../g/gettoken_query_websearch.md)
  - P_TSQ_WEB

## Notes and Other Information
- This function creates the minimal possible QueryOperand structure with only the type field initialized
- Stopword placeholders are used to maintain query structure while marking non-searchable terms
- The QI_VALSTOP type distinguishes these placeholders from regular searchable operands (QI_VAL)
- Unlike pushValue functions, this function doesn't need to handle string content, CRC calculation, or buffer management
- Stopword handling is an important optimization in text search systems to improve both performance and relevance
- The function is typically called during morphological analysis when the text search dictionary identifies a word as a stopword
- Memory allocation uses palloc0 to ensure proper zero-initialization, though only the type field is explicitly set

## Simplified Source

```c
void pushStop(TSQueryParserState state) {
    // Create stopword placeholder operand
    QueryOperand *tmp = (QueryOperand *) palloc0(sizeof(QueryOperand));
    tmp->type = QI_VALSTOP;

    // Add placeholder to polish notation list
    state->polstr = lcons(tmp, state->polstr);
}
```