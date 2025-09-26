# TSQueryParserStateData

## Location
[src/backend/utils/adt/tsquery.c:78-113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery.c#L78-L113)

## Overview
TSQueryParserStateData is a comprehensive parser state structure that maintains all necessary information during tsquery parsing operations, including tokenization state, operator management, and error handling context.

## Definition
```c
struct TSQueryParserStateData
{
    /* Tokenizer used for parsing tsquery */
    ts_tokenizer gettoken;

    /* State of tokenizer function */
    char       *buffer;         /* entire string we are scanning */
    char       *buf;            /* current scan point */
    int         count;          /* nesting count, incremented by (,
                                * decremented by ) */
    ts_parserstate state;

    /* polish (prefix) notation in list, filled in by push* functions */
    List       *polstr;

    /*
     * Strings from operands are collected in op. curop is a pointer to the
     * end of used space of op.
     */
    char       *op;
    char       *curop;
    int         lenop;          /* allocated size of op */
    int         sumlen;         /* used size of op */

    /* state for value's parser */
    TSVectorParseState valstate;

    /* context object for soft errors - must match valstate's escontext */
    Node       *escontext;
};
```

## Detailed Description
TSQueryParserStateData serves as the central state container for parsing tsquery expressions in PostgreSQL's full-text search functionality. The structure orchestrates the complex process of converting text-based tsquery expressions into internal query representations. It manages tokenization through the gettoken function pointer, maintains parsing position through buffer pointers, tracks nesting levels for parentheses, and accumulates operands and operators in Polish notation format. The structure also integrates with TSVector parsing capabilities and provides error context management for graceful error handling during parsing operations.

## Parameters / Member Variables
- `gettoken`: Function pointer to the tokenizer used for parsing tsquery expressions
- `buffer`: Pointer to the entire input string being scanned
- `buf`: Current scanning position within the buffer
- `count`: Nesting level counter, incremented by opening parentheses and decremented by closing parentheses
- `state`: Current state of the parser state machine
- `polstr`: List containing the parsed expression in Polish (prefix) notation
- `op`: Buffer for collecting operand strings
- `curop`: Pointer to the end of used space in the op buffer
- `lenop`: Total allocated size of the op buffer
- `sumlen`: Currently used size of the op buffer
- `valstate`: Parser state for processing individual values/operands
- `escontext`: Error context node for soft error handling, must match valstate's escontext

## Dependencies
- Functions called/Symbols referenced:
  - ts_parserstate
  - TSVectorParseState
- Called from (representative examples):
  - parse_tsquery
  - ISOPERATOR
  - TSQueryParserState

## Notes and Other Information
This structure is fundamental to PostgreSQL's full-text search parsing infrastructure. The Polish notation representation (polstr) enables efficient query evaluation by maintaining operators and operands in prefix order. The dual buffer management system (buffer/buf for input, op/curop for operands) allows for efficient string processing during parsing. The integration with TSVectorParseState ensures consistency between query and document parsing operations. Error context management through escontext enables robust error reporting without breaking parser state.