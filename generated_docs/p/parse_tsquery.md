# parse_tsquery

## Location
[src/backend/utils/adt/tsquery.c:817-941](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery.c#L817-L941)

## Overview
The  function parses a text search query string and converts it into PostgreSQL's internal TSQuery representation, handling different query formats (standard, plain text, and websearch) with customizable processing callbacks.

## Definition

```c
struct TSQueryParserStateData state;
```
## Detailed Description
This function is the core parser for PostgreSQL's text search queries. It takes a query string and transforms it into an internal TSQuery structure that can be efficiently executed against tsvector data. The function supports multiple parsing modes:

- **Standard mode**: Traditional PostgreSQL tsquery syntax with operators (&, |, \!, <->)
- **Plain text mode**: Simple text without operators, treating input as phrase search
- **Websearch mode**: Google-like search syntax with quoted phrases and simple operators

The parser uses a callback mechanism () to process individual query terms, allowing for extensibility and customization. It builds the query in polish notation (postfix) internally, then converts it to the final TSQuery format. The function includes comprehensive error handling with soft error support and can handle stopword cleanup automatically.

## Parameters / Member Variables
- : Input query string to be parsed
- : Callback function to process individual query operands/values
- : Opaque data passed through to the pushval callback function
- : Bitmask controlling parsing behavior (P_TSQ_PLAIN, P_TSQ_WEB, etc.)
- : Error context for soft error handling, can be NULL for hard errors

## Dependencies
- Functions called/Symbols referenced:
  - [init_tsvector_parser](../i/init_tsvector_parser.md)
  - [makepol](../m/makepol.md)
  - [close_tsvector_parser](../c/close_tsvector_parser.md)
  - [findoprnd](../f/findoprnd.md)
  - [cleanup_tsquery_stopwords](../c/cleanup_tsquery_stopwords.md)
  - [gettoken_query_plain](../g/gettoken_query_plain.md)
  - [gettoken_query_websearch](../g/gettoken_query_websearch.md)
  - [gettoken_query_standard](../g/gettoken_query_standard.md)
- Called from (representative examples):
  - [tsqueryin](../t/tsqueryin.md)
  - [to_tsquery_byid](../t/to_tsquery_byid.md)
  - [plainto_tsquery_byid](plainto_tsquery_byid.md)
  - [phraseto_tsquery_byid](phraseto_tsquery_byid.md)
  - [websearch_to_tsquery_byid](../w/websearch_to_tsquery_byid.md)

## Notes and Other Information
- The function validates that incompatible flags (P_TSQ_PLAIN and P_TSQ_WEB) are not used together
- Returns NULL if soft errors occur and escontext is provided
- Emits NOTICE messages for empty queries only when not in soft error mode
- Automatically handles memory management for the internal parsing structures
- The resulting TSQuery includes both the parsed structure and operand strings in a single allocation
- Stopword nodes (QI_VALSTOP) are automatically cleaned up if present in the final query tree