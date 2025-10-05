# tsquery_opr_selec

## Location
[src/backend/tsearch/ts_selfuncs.c:278-433](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/ts_selfuncs.c#L278-L433)

## Overview
Recursively traverses TSQuery trees to compute selectivity estimates using statistics-based analysis for lexemes and probability theory for logical operators.

## Definition

```c
static Selectivity
tsquery_opr_selec(QueryItem *item, char *operand,
				  TextFreq *lookup, int length, float4 minfreq)
```
## Detailed Description
 is the core recursive function that analyzes TSQuery expression trees to estimate selectivity. It implements a sophisticated algorithm that handles different types of query nodes:

**For lexeme nodes (QI_VAL):**
- **Exact matches**: Uses binary search in the MCELEM statistics to find precise frequencies, or falls back to estimated frequencies for uncommon terms
- **Prefix matches**: Scans through all MCELEMs to find matching prefixes and extrapolates to the entire lexeme population

**For operator nodes:**
- **AND/PHRASE**: Multiplies selectivities (assumes independence)
- **OR**: Uses inclusion-exclusion principle: 
- **NOT**: Complements selectivity: 

The function gracefully handles cases with insufficient or missing statistics by falling back to default estimates. It includes stack depth checking for protection against deeply nested queries.

## Parameters / Member Variables
- `*item`: Current QueryItem in the TSQuery tree being processed
- `*operand`: String buffer containing all lexeme text for the query
- `*lookup`: Array of TextFreq structures containing most-common-elements statistics
- `length`: Number of elements in the lookup array
- `minfreq`: Minimum frequency from statistics, used for estimation bounds
## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md): Prevents stack overflow from deep recursion
  - bsearch: Binary search for exact lexeme matches in statistics
  - [compare_lexeme_textfreq](../c/compare_lexeme_textfreq.md): Comparison function for binary search
  - strncmp: String comparison for prefix matching
  - CLAMP_PROBABILITY: Ensures selectivity stays within [0,1] bounds
  - elog: Error logging for unrecognized operators
- Data structures used:
  - QueryItem: TSQuery tree node structure
  - QueryOperand: Lexeme node with distance, length, and prefix flag
  - [TextFreq](../T/TextFreq.md): Pairing of lexeme text with frequency statistics
  - [LexemeKey](../L/LexemeKey.md): Search key structure for binary search
- Constants used:
  - QI_VAL: Query item type for lexeme nodes
  - OP_NOT/OP_AND/OP_PHRASE/OP_OR: Operator type constants
  - DEFAULT_TS_MATCH_SEL: Default selectivity for unknown lexemes
- Macros used:
  - VARSIZE_ANY_EXHDR/VARDATA_ANY: Text data access macros
- Called from (representative examples):
  - [mcelem_tsquery_selec](../m/mcelem_tsquery_selec.md): Entry point with statistics
  - Self-recursive calls for operator evaluation

## Notes and Other Information
- Implements a recursive descent parser for TSQuery expression trees
- Uses probability theory principles for combining operator selectivities
- Handles prefix queries by scanning and extrapolating from MCELEM statistics
- Requires at least 100 MCELEM entries for reliable prefix estimation
- For prefix matches, ensures selectivity is at least as high as exact matches would be
- Uses independent assumption for AND operations (may underestimate for correlated terms)
- Includes comprehensive bounds checking and error handling
- Central to PostgreSQL's cost-based optimization for full-text search queries
- The algorithm assumes that MCELEM statistics are representative of the overall lexeme distribution

## Simplified Source

```c
static Selectivity tsquery_opr_selec(QueryItem *item, char *operand,
                                    TextFreq *lookup, int length, float4 minfreq) {
    Selectivity selec;

    check_stack_depth();

    if (item->type == QI_VAL) {
        QueryOperand *oper = (QueryOperand *) item;
        LexemeKey key;

        key.lexeme = operand + oper->distance;
        key.length = oper->length;

        if (oper->prefix) {
            // Prefix match: scan all MCELEMs for matches
            if (lookup == NULL || length < 100)
                return (Selectivity) (DEFAULT_TS_MATCH_SEL * 4);

            Selectivity matched = 0, allmces = 0;
            int n_matched = 0;

            for (int i = 0; i < length; i++) {
                TextFreq *t = lookup + i;
                int tlen = VARSIZE_ANY_EXHDR(t->element);

                if (tlen >= key.length &&
                    strncmp(key.lexeme, VARDATA_ANY(t->element), key.length) == 0) {
                    matched += t->frequency - matched * t->frequency;
                    n_matched++;
                }
                allmces += t->frequency - allmces * t->frequency;
            }

            CLAMP_PROBABILITY(matched);
            CLAMP_PROBABILITY(allmces);

            selec = matched + (1.0 - allmces) * ((double) n_matched / length);
            selec = Max(Min(DEFAULT_TS_MATCH_SEL, minfreq / 2), selec);
        } else {
            // Exact lexeme match
            if (lookup == NULL)
                return (Selectivity) DEFAULT_TS_MATCH_SEL;

            TextFreq *searchres = bsearch(&key, lookup, length,
                                        sizeof(TextFreq), compare_lexeme_textfreq);

            if (searchres) {
                selec = searchres->frequency;
            } else {
                selec = Min(DEFAULT_TS_MATCH_SEL, minfreq / 2);
            }
        }
    } else {
        // Operator node
        Selectivity s1, s2;

        switch (item->qoperator.oper) {
            case OP_NOT:
                selec = 1.0 - tsquery_opr_selec(item + 1, operand,
                                               lookup, length, minfreq);
                break;

            case OP_PHRASE:
            case OP_AND:
                s1 = tsquery_opr_selec(item + 1, operand, lookup, length, minfreq);
                s2 = tsquery_opr_selec(item + item->qoperator.left, operand,
                                     lookup, length, minfreq);
                selec = s1 * s2;
                break;

            case OP_OR:
                s1 = tsquery_opr_selec(item + 1, operand, lookup, length, minfreq);
                s2 = tsquery_opr_selec(item + item->qoperator.left, operand,
                                     lookup, length, minfreq);
                selec = s1 + s2 - s1 * s2;
                break;

            default:
                elog(ERROR, "unrecognized operator: %d", item->qoperator.oper);
                selec = 0;
                break;
        }
    }

    CLAMP_PROBABILITY(selec);
    return selec;
}
```