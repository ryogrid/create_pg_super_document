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
- `state`: The TSVectorParseState containing parser configuration and current state
- `**strval`: Output parameter - pointer to the extracted token string
- `*lenval`: Output parameter - length of the extracted token
- `**pos_ptr`: Output parameter - array of positions and weights (caller must pfree), NULL if not needed
- `*poslen`: Output parameter - number of elements in pos_ptr array
- `**endptr`: Output parameter - scan resumption point for continued parsing
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

## Simplified Source

```c
bool gettoken_tsvector(TSVectorParseState state, char **strval, int *lenval,
                      WordEntryPos **pos_ptr, int *poslen, char **endptr) {
    int statecode = WAITWORD;
    char *curpos = state->word;
    WordEntryPos *pos = NULL;
    int npos = 0, posalen = 0;

    while (1) {
        switch (statecode) {
            case WAITWORD:
                // Handle start of word: quoted, escaped, or regular
                if (*(state->prsbuf) == '\0')
                    return false;  // End of input
                else if (!state->is_web && t_iseq(state->prsbuf, '\''))
                    statecode = WAITENDCMPLX;  // Quoted word
                else if (!state->is_web && t_iseq(state->prsbuf, '\\'))
                    statecode = WAITNEXTCHAR;  // Escaped character
                else if (!t_isspace(state->prsbuf)) {
                    // Regular word character
                    COPYCHAR(curpos, state->prsbuf);
                    curpos += pg_mblen(state->prsbuf);
                    statecode = WAITENDWORD;
                }
                break;

            case WAITENDWORD:
                // Process regular word until delimiter or position info
                if (t_isspace(state->prsbuf) || *(state->prsbuf) == '\0' ||
                    (state->oprisdelim && ISOPERATOR(state->prsbuf))) {
                    // End of word
                    if (curpos == state->word) PRSSYNTAXERROR;
                    *(curpos) = '\0';
                    RETURN_TOKEN;
                } else if (t_iseq(state->prsbuf, ':')) {
                    // Position information follows
                    *(curpos) = '\0';
                    statecode = state->oprisdelim ? RETURN_TOKEN : INPOSINFO;
                } else {
                    // Continue word
                    COPYCHAR(curpos, state->prsbuf);
                    curpos += pg_mblen(state->prsbuf);
                }
                break;

            case WAITENDCMPLX:
                // Handle quoted complex words
                if (!state->is_web && t_iseq(state->prsbuf, '\''))
                    statecode = WAITCHARCMPLX;
                else if (*(state->prsbuf) == '\0')
                    PRSSYNTAXERROR;
                else {
                    COPYCHAR(curpos, state->prsbuf);
                    curpos += pg_mblen(state->prsbuf);
                }
                break;

            case INPOSINFO:
                // Parse position numbers
                if (t_isdigit(state->prsbuf)) {
                    // Allocate/expand position array if needed
                    if (posalen == 0) {
                        posalen = 4;
                        pos = (WordEntryPos *) palloc(sizeof(WordEntryPos) * posalen);
                        npos = 0;
                    }
                    // Add position
                    WEP_SETPOS(pos[npos], LIMITPOS(atoi(state->prsbuf)));
                    WEP_SETWEIGHT(pos[npos], 0);
                    npos++;
                    statecode = WAITPOSDELIM;
                } else {
                    PRSSYNTAXERROR;
                }
                break;

            case WAITPOSDELIM:
                // Handle position delimiters and weights
                if (t_iseq(state->prsbuf, ','))
                    statecode = INPOSINFO;  // More positions
                else if (t_iseq(state->prsbuf, 'A') || t_iseq(state->prsbuf, '*'))
                    WEP_SETWEIGHT(pos[npos - 1], 3);  // Weight A
                else if (t_iseq(state->prsbuf, 'B'))
                    WEP_SETWEIGHT(pos[npos - 1], 2);  // Weight B
                else if (t_iseq(state->prsbuf, 'C'))
                    WEP_SETWEIGHT(pos[npos - 1], 1);  // Weight C
                else if (t_iseq(state->prsbuf, 'D'))
                    WEP_SETWEIGHT(pos[npos - 1], 0);  // Weight D
                else if (t_isspace(state->prsbuf) || *(state->prsbuf) == '\0')
                    RETURN_TOKEN;
                break;

            // Additional states for escaped chars and complex word handling...
            default:
                elog(ERROR, "unrecognized state in gettoken_tsvector: %d", statecode);
        }

        // Advance to next character
        state->prsbuf += pg_mblen(state->prsbuf);
    }
}
```