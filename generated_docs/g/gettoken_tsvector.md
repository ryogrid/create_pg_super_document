# gettoken_tsvector

## Location
[src/backend/utils/adt/tsvector_parser.c:176-388](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_parser.c#L176-L388)

## Overview
Parses and extracts the next token from a tsvector or tsquery input string, including associated positional and weight information.

## Definition

```c
bool
gettoken_tsvector(TSVectorParseState state,
				  char **strval, int *lenval,
				  WordEntryPos **pos_ptr, int *poslen,
				  char **endptr)
```
## Detailed Description
This is the core parsing function that implements a finite state machine to tokenize tsvector and tsquery input strings. It handles various token types including simple words, quoted complex words, escaped characters, and positional information with weights. The parser supports different modes (web search, tsquery, operator-as-delimiter) and can extract position and weight information associated with tokens. The function returns true on successful token extraction and false at end-of-input or on soft errors.

## Parameters / Member Variables
- : The TSVectorParseState containing parser configuration and current state
- : Output parameter - pointer to the extracted token string
- : Output parameter - length of the extracted token
- : Output parameter - array of positions and weights (caller must pfree), NULL if not needed
- : Output parameter - number of elements in pos_ptr array
- : Output parameter - scan resumption point for continued parsing

## Dependencies
- Functions called/Symbols referenced:
  - t_iseq, t_isspace, t_isdigit
  - COPYCHAR, RESIZEPRSBUF, RETURN_TOKEN
  - PRSSYNTAXERROR, ISOPERATOR
  - [pg_mblen](../p/pg_mblen.md), palloc, repalloc
  - WEP_SETPOS, WEP_GETPOS, WEP_SETWEIGHT, WEP_GETWEIGHT
  - LIMITPOS, ereturn, elog
- Called from (representative examples):
  - [tsvectorin](../t/tsvectorin.md) (src/backend/utils/adt/tsvector.c:208)
  - [gettoken_query_standard](gettoken_query_standard.md) (src/backend/utils/adt/tsquery.c:325)
  - [gettoken_query_websearch](gettoken_query_websearch.md) (src/backend/utils/adt/tsquery.c:454)

## Notes and Other Information
The parser implements a comprehensive state machine with states including WAITWORD, WAITENDWORD, WAITENDCMPLX, WAITCHARCMPLX, WAITPOSINFO, WAITPOSDELIM, INPOSINFO, and WAITNEXTCHAR. It handles multibyte characters correctly and supports position information in the format 'word:1,2,3A,4B' where numbers are positions and letters are weights (A/*, B, C, D). The function supports both hard and soft error handling through the error context system.