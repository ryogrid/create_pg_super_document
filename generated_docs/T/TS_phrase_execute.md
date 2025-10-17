# TS_phrase_execute

## Location
[src/backend/utils/adt/tsvector_op.c:1609-1853](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L1609-L1853)

## Overview
TS_phrase_execute is a recursive function that executes tsquery operations at or below an OP_PHRASE operator, handling text search execution at recursion levels where match locations are crucial for phrase matching and position-aware operations.

## Definition

```c
static TSTernaryValue
TS_phrase_execute(QueryItem *curitem, void *arg, uint32 flags,
				  TSExecuteCallback chkcond,
				  ExecPhraseData *data)
```
## Detailed Description
This function is the core execution engine for PostgreSQL's text search phrase queries, designed to handle complex boolean logic while tracking lexeme positions for phrase matching. It recursively processes query trees containing OP_PHRASE, OP_AND, OP_OR, and OP_NOT operations, maintaining detailed position information required for proximity-based text searches.

The function implements sophisticated position semantics:
- For successful matches with npos > 0 and negate = false: query matches at specified positions only
- For npos > 0 and negate = true: query matches everywhere except specified positions  
- For npos = 0 and negate = true: query matches at all positions
- Returns a "width" value representing match width in lexemes minus one

Key behaviors include:
- Stack depth checking to prevent overflow during deep recursion
- Interrupt handling for query cancellation
- Position data management through ExecPhraseData structures
- Complex boolean logic handling with position propagation
- Phrase distance calculation and width computation

## Parameters / Member Variables
- `*curitem`: Pointer to the current QueryItem being processed in the query tree
- `*arg`: Opaque argument passed to the TSExecuteCallback function
- `flags`: Execution flags controlling behavior (e.g., TS_EXEC_SKIP_NOT)
- `chkcond`: Callback function to check if a lexeme condition is satisfied
- `*data`: Pointer to ExecPhraseData structure for position information (NULL if positions not needed)
## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md)
  - CHECK_FOR_INTERRUPTS
  - TS_phrase_output
  - chkcond (callback)
  - memset
  - elog
- Called from (representative examples):
  - [TS_phrase_execute](TS_phrase_execute.md) (recursive calls)
  - [TS_execute_recurse](TS_execute_recurse.md)
  - [TS_execute_locations_recurse](TS_execute_locations_recurse.md)

## Notes and Other Information
- The function is designed to be recursively safe with stack depth monitoring
- Handles De Morgan's law transformations for negated boolean operations (\!L & \!R becomes \!(L | R))
- Position alignment logic ensures consistent width reporting across different operator types
- The width calculation follows the rule that positions represent match ends rather than starts when width > 0
- Critical for phrase search functionality where word proximity and order matter
- Returns TSTernaryValue (TS_YES, TS_NO, TS_MAYBE) to handle uncertain match scenarios

## Simplified Source

```c
static TSTernaryValue TS_phrase_execute(QueryItem *curitem, void *arg, uint32 flags,
                                       TSExecuteCallback chkcond, ExecPhraseData *data) {
    ExecPhraseData Ldata, Rdata;
    TSTernaryValue lmatch, rmatch;
    int Loffset, Roffset, maxwidth;

    // Safety checks
    check_stack_depth();
    CHECK_FOR_INTERRUPTS();

    // Base case: evaluate leaf node (operand)
    if (curitem->type == QI_VAL)
        return chkcond(arg, (QueryOperand *) curitem, data);

    // Handle different operators
    switch (curitem->qoperator.oper) {
        case OP_NOT:
            if (flags & TS_EXEC_SKIP_NOT) {
                // Report NOT as "match everywhere"
                Assert(data->npos == 0 && !data->negate);
                data->negate = true;
                return TS_YES;
            }

            // Recursively evaluate the negated operand
            switch (TS_phrase_execute(curitem + 1, arg, flags, chkcond, data)) {
                case TS_NO:
                    // "match nowhere" becomes "match everywhere"
                    data->negate = true;
                    return TS_YES;
                case TS_YES:
                    if (data->npos > 0) {
                        // Invert the negate flag
                        data->negate = !data->negate;
                        return TS_YES;
                    } else if (data->negate) {
                        // "match everywhere" becomes "match nowhere"
                        data->negate = false;
                        return TS_NO;
                    }
                    break;
                case TS_MAYBE:
                    return TS_MAYBE;
            }
            break;

        case OP_PHRASE:
        case OP_AND:
            // Initialize left and right data structures
            memset(&Ldata, 0, sizeof(Ldata));
            memset(&Rdata, 0, sizeof(Rdata));

            // Evaluate left and right operands
            lmatch = TS_phrase_execute(curitem + curitem->qoperator.left, arg, flags, chkcond, &Ldata);
            if (lmatch == TS_NO) return TS_NO;

            rmatch = TS_phrase_execute(curitem + 1, arg, flags, chkcond, &Rdata);
            if (rmatch == TS_NO) return TS_NO;

            // Handle uncertain matches
            if (lmatch == TS_MAYBE || rmatch == TS_MAYBE)
                return TS_MAYBE;

            // Calculate position offsets and width
            if (curitem->qoperator.oper == OP_PHRASE) {
                // Phrase: compute distance-based offsets
                Loffset = curitem->qoperator.distance + Rdata.width;
                Roffset = 0;
                if (data)
                    data->width = curitem->qoperator.distance + Ldata.width + Rdata.width;
            } else {
                // AND: align to maximum width
                maxwidth = Max(Ldata.width, Rdata.width);
                Loffset = maxwidth - Ldata.width;
                Roffset = maxwidth - Rdata.width;
                if (data)
                    data->width = maxwidth;
            }

            // Handle negated operands using boolean logic
            if (Ldata.negate && Rdata.negate) {
                // !L & !R: treat as !(L | R)
                TS_phrase_output(data, &Ldata, &Rdata,
                               TSPO_BOTH | TSPO_L_ONLY | TSPO_R_ONLY,
                               Loffset, Roffset, Ldata.npos + Rdata.npos);
                if (data) data->negate = true;
                return TS_YES;
            } else if (Ldata.negate) {
                // !L & R
                return TS_phrase_output(data, &Ldata, &Rdata, TSPO_R_ONLY,
                                      Loffset, Roffset, Rdata.npos);
            } else if (Rdata.negate) {
                // L & !R
                return TS_phrase_output(data, &Ldata, &Rdata, TSPO_L_ONLY,
                                      Loffset, Roffset, Ldata.npos);
            } else {
                // Straight AND
                return TS_phrase_output(data, &Ldata, &Rdata, TSPO_BOTH,
                                      Loffset, Roffset, Min(Ldata.npos, Rdata.npos));
            }

        case OP_OR:
            // Initialize data structures
            memset(&Ldata, 0, sizeof(Ldata));
            memset(&Rdata, 0, sizeof(Rdata));

            // Evaluate both operands
            lmatch = TS_phrase_execute(curitem + curitem->qoperator.left, arg, flags, chkcond, &Ldata);
            rmatch = TS_phrase_execute(curitem + 1, arg, flags, chkcond, &Rdata);

            if (lmatch == TS_NO && rmatch == TS_NO)
                return TS_NO;

            if (lmatch == TS_MAYBE || rmatch == TS_MAYBE)
                return TS_MAYBE;

            // Handle undefined widths from failed matches
            if (lmatch == TS_NO) Ldata.width = 0;
            if (rmatch == TS_NO) Rdata.width = 0;

            // Align to maximum width
            maxwidth = Max(Ldata.width, Rdata.width);
            Loffset = maxwidth - Ldata.width;
            Roffset = maxwidth - Rdata.width;
            data->width = maxwidth;

            // Handle negated operands using boolean logic
            if (Ldata.negate && Rdata.negate) {
                // !L | !R: treat as !(L & R)
                TS_phrase_output(data, &Ldata, &Rdata, TSPO_BOTH,
                               Loffset, Roffset, Min(Ldata.npos, Rdata.npos));
                data->negate = true;
                return TS_YES;
            } else if (Ldata.negate) {
                // !L | R: treat as !(L & !R)
                TS_phrase_output(data, &Ldata, &Rdata, TSPO_L_ONLY,
                               Loffset, Roffset, Ldata.npos);
                data->negate = true;
                return TS_YES;
            } else if (Rdata.negate) {
                // L | !R: treat as !(!L & R)
                TS_phrase_output(data, &Ldata, &Rdata, TSPO_R_ONLY,
                               Loffset, Roffset, Rdata.npos);
                data->negate = true;
                return TS_YES;
            } else {
                // Straight OR
                return TS_phrase_output(data, &Ldata, &Rdata,
                                      TSPO_BOTH | TSPO_L_ONLY | TSPO_R_ONLY,
                                      Loffset, Roffset, Ldata.npos + Rdata.npos);
            }

        default:
            elog(ERROR, "unrecognized operator: %d", curitem->qoperator.oper);
    }

    return TS_NO;
}
```