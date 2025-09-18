# HeadlineParsedText

## Location
src/include/tsearch/ts_public.h: 89 - 103

## Overview
HeadlineParsedText is a structure that contains the complete data needed for text search headline generation, including an array of parsed word tokens and formatting strings for highlighting and fragment separation.

## Definition
```c
typedef struct
{
    /* Fields filled by core code before calling prsheadline function: */
    HeadlineWordEntry *words;
    int32       lenwords;       /* allocated length of words[] */
    int32       curwords;       /* current number of valid entries */
    int32       vectorpos;      /* used by ts_parse.c in filling pos fields */

    /* The prsheadline function must fill these fields: */
    /* Strings for marking selected tokens and separating fragments: */
    char       *startsel;       /* palloc'd strings */
    char       *stopsel;
    char       *fragdelim;
    int16       startsellen;    /* lengths of strings */
    int16       stopsellen;
    int16       fragdelimlen;
} HeadlineParsedText;
```

## Detailed Description
HeadlineParsedText serves as the primary data structure for PostgreSQL's text search headline generation system. It acts as a container that holds both the parsed word tokens and the formatting information needed to generate highlighted text snippets.

The structure has two distinct phases of use: first, the core parsing code fills in the word array and related metadata, then the prsheadline function processes this data and fills in the formatting strings. This two-phase approach separates the concerns of text parsing from headline formatting, allowing different headline generation strategies to work with the same parsed data.

The words array contains all the tokens from the input text, with each HeadlineWordEntry containing information about whether it should be highlighted, its position, and any matching query operands. The formatting strings (startsel, stopsel, fragdelim) are used to mark highlighted tokens and separate text fragments in the final output.

## Parameters / Member Variables
- `words`: Dynamic array of HeadlineWordEntry structures containing parsed tokens
- `lenwords`: Total allocated length of the words array
- `curwords`: Current number of valid entries in the words array  
- `vectorpos`: Position counter used by ts_parse.c when filling position fields in word entries
- `startsel`: String to insert before highlighted tokens (dynamically allocated)
- `stopsel`: String to insert after highlighted tokens (dynamically allocated)
- `fragdelim`: String used to separate text fragments in output (dynamically allocated)
- `startsellen`: Length of the startsel string
- `stopsellen`: Length of the stopsel string
- `fragdelimlen`: Length of the fragdelim string

## Dependencies
- Functions called/Symbols referenced:
  - [HeadlineWordEntry](HeadlineWordEntry.md)
- Called from (representative examples):
  - [hladdword](../h/hladdword.md) (src/backend/tsearch/ts_parse.c:440)
  - [hlparsetext](../h/hlparsetext.md) (src/backend/tsearch/ts_parse.c:540)
  - [generateHeadline](../g/generateHeadline.md) (src/backend/tsearch/ts_parse.c:607)
  - [ts_headline_byid_opt](../t/ts_headline_byid_opt.md) (src/backend/tsearch/wparser.c:294)
  - [prsd_headline](../p/prsd_headline.md) (src/backend/tsearch/wparser_def.c:2618)

## Notes and Other Information
- The structure uses a two-phase initialization: core code fills word-related fields, then prsheadline functions fill formatting fields
- All formatting strings (startsel, stopsel, fragdelim) are palloc'd and must be properly freed
- The words array can be dynamically resized as needed during parsing
- The vectorpos field is specifically used by ts_parse.c for maintaining position information consistency
- This structure is commonly used in conjunction with TSQuery for matching and highlighting search terms
- Different headline generation strategies can use the same parsed data by implementing different prsheadline functions