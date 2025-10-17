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

## Simplified Source

```c
TSQuery
parse_tsquery(char *buf, PushFunction pushval, Datum opaque, int flags, Node *escontext)
{
    struct TSQueryParserStateData state;
    TSQuery query;
    int commonlen;
    QueryItem *ptr;
    ListCell *cell;
    bool noisy;
    bool needcleanup;
    int tsv_flags = P_TSV_OPR_IS_DELIM | P_TSV_IS_TSQUERY;

    // Validate flags - plain and web modes are mutually exclusive
    Assert((flags & (P_TSQ_PLAIN | P_TSQ_WEB)) != (P_TSQ_PLAIN | P_TSQ_WEB));

    // Select appropriate tokenizer based on parsing mode
    if (flags & P_TSQ_PLAIN)
        state.gettoken = gettoken_query_plain;
    else if (flags & P_TSQ_WEB)
    {
        state.gettoken = gettoken_query_websearch;
        tsv_flags |= P_TSV_IS_WEB;
    }
    else
        state.gettoken = gettoken_query_standard;

    // Initialize parser state
    noisy = !(escontext && IsA(escontext, ErrorSaveContext));
    state.buffer = buf;
    state.buf = buf;
    state.count = 0;
    state.state = WAITFIRSTOPERAND;
    state.polstr = NIL;
    state.escontext = escontext;

    // Initialize value parser and operand storage
    state.valstate = init_tsvector_parser(state.buffer, tsv_flags, escontext);
    state.sumlen = 0;
    state.lenop = 64;
    state.curop = state.op = (char *) palloc(state.lenop);
    *(state.curop) = '\0';

    // Parse query into polish notation
    makepol(&state, pushval, opaque);
    close_tsvector_parser(state.valstate);

    if (SOFT_ERROR_OCCURRED(escontext))
        return NULL;

    // Handle empty query case
    if (state.polstr == NIL)
    {
        if (noisy)
            ereport(NOTICE, (errmsg("text-search query doesn't contain lexemes: \"%s\"", state.buffer)));

        query = (TSQuery) palloc(HDRSIZETQ);
        SET_VARSIZE(query, HDRSIZETQ);
        query->size = 0;
        return query;
    }

    // Check size limits and allocate result structure
    if (TSQUERY_TOO_BIG(list_length(state.polstr), state.sumlen))
        ereturn(escontext, NULL, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                errmsg("tsquery is too large")));

    commonlen = COMPUTESIZE(list_length(state.polstr), state.sumlen);
    query = (TSQuery) palloc0(commonlen);
    SET_VARSIZE(query, commonlen);
    query->size = list_length(state.polstr);
    ptr = GETQUERY(query);

    // Copy parsed query items to result structure
    int i = 0;
    foreach(cell, state.polstr)
    {
        QueryItem *item = (QueryItem *) lfirst(cell);
        switch (item->type)
        {
            case QI_VAL:
                memcpy(&ptr[i], item, sizeof(QueryOperand));
                break;
            case QI_VALSTOP:
                ptr[i].type = QI_VALSTOP;
                break;
            case QI_OPR:
                memcpy(&ptr[i], item, sizeof(QueryOperator));
                break;
            default:
                elog(ERROR, "unrecognized QueryItem type: %d", item->type);
        }
        i++;
    }

    // Copy operand strings and clean up
    memcpy(GETOPERAND(query), state.op, state.sumlen);
    pfree(state.op);

    // Fill operator offsets and detect stop words
    findoprnd(ptr, query->size, &needcleanup);

    // Remove stop words if present
    if (needcleanup)
        query = cleanup_tsquery_stopwords(query, noisy);

    return query;
}
```