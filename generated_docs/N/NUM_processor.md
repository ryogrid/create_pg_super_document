# NUM_processor

## Location
[src/backend/utils/adt/formatting.c:5823-6306](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L5823-L6306)

## Overview
The core formatting engine that processes number formatting patterns and converts between numeric values and their textual representations in PostgreSQL's format system.

## Definition

```c
static char *
NUM_processor(FormatNode *node, NUMDesc *Num, char *inout,
			  char *number, int input_len, int to_char_out_pre_spaces,
			  int sign, bool is_to_char, Oid collid)
```
## Detailed Description
NUM_processor is the central function in PostgreSQL's number formatting system. It processes format patterns defined by FormatNode structures and converts between numeric strings and formatted text representations. The function handles both TO_CHAR (number to formatted string) and TO_NUMBER (formatted string to number) operations.

The function manages complex formatting requirements including:
- Roman numeral conversion (RN/rn)
- Scientific notation (EEEE) 
- Sign handling (MI, PL, SG)
- Decimal formatting with locale support
- Fill mode operations
- Thousands separators and currency symbols
- Ordinal suffixes (th/TH)

The function operates in two main phases: initialization/setup and pattern processing. During setup, it configures the NUMProc structure with formatting parameters. During processing, it iterates through format nodes and applies the appropriate transformations.

## Parameters / Member Variables
- `*node`: Array of FormatNode structures defining the format pattern
- `*Num`: NUMDesc structure containing format specifications and flags
- `*inout`: Input/output buffer for the formatted string
- `*number`: Numeric string to be processed
- `input_len`: Length of input buffer for boundary checking
- `to_char_out_pre_spaces`: Number of leading spaces in TO_CHAR output
- `sign`: Sign character for the number ('+', '-', or space)
- `is_to_char`: Boolean indicating direction (true for TO_CHAR, false for TO_NUMBER)
- `collid`: Collation identifier for locale-specific formatting
## Dependencies
- Functions called/Symbols referenced:
  - MemSet, IS_EEEE, IS_ROMAN, IS_FILLMODE, IS_DECIMAL, IS_ZERO
  - [NUM_prepare_locale](NUM_prepare_locale.md), NUM_numpart_to_char, NUM_numpart_from_char
  - [NUM_eat_non_data_chars](NUM_eat_non_data_chars.md), get_th, get_last_relevant_decnum
  - [pg_mblen](../p/pg_mblen.md), pg_mbstrlen, asc_tolower_z, OVERLOAD_TEST, AMOUNT_TEST
- Called from (representative examples):
  - DCH_ZONED (at formatting.c:1081)
  - NUM_TOCHAR_finish (at formatting.c:6324)  
  - [numeric_to_number](../n/numeric_to_number.md) (at formatting.c:6365)

## Notes and Other Information
- This is a static function, only available within formatting.c
- Supports extensive debugging output when DEBUG_TO_FROM_CHAR is defined
- Handles multibyte character encodings properly
- Contains comprehensive error checking for unsupported format combinations
- The function is highly optimized with continue/break statements to minimize unnecessary processing
- Roman numeral and scientific notation formats are handled as special cases
- Locale-aware formatting uses system locale settings for currency and thousands separators
- Pattern processing loop handles both format actions and literal characters differently

## Simplified Source

```c
static char *NUM_processor(FormatNode *node, NUMDesc *Num, char *inout,
                          char *number, int input_len, int to_char_out_pre_spaces,
                          int sign, bool is_to_char, Oid collid) {
    FormatNode *n;
    NUMProc _Np, *Np = &_Np;
    const char *pattern;
    int pattern_len;

    // Initialize processing context
    MemSet(Np, 0, sizeof(NUMProc));
    Np->Num = Num;
    Np->is_to_char = is_to_char;
    Np->number = number;
    Np->inout = inout;

    // Handle special format types
    if (IS_EEEE(Np->Num)) {
        if (!Np->is_to_char)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           errmsg("\"EEEE\" not supported for input")));
        return strcpy(inout, number);
    }

    if (IS_ROMAN(Np->Num)) {
        if (!Np->is_to_char)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           errmsg("\"RN\" not supported for input")));
        // Reset format flags for Roman numerals
        Np->Num->lsign = Np->Num->pre_lsign_num = Np->Num->post =
                        Np->Num->pre = Np->out_pre_spaces = Np->sign = 0;
    }

    // Set up sign handling and counting
    if (is_to_char) {
        Np->sign = sign;
        Np->out_pre_spaces = to_char_out_pre_spaces;
        // Handle sign placement logic
        if (IS_PLUS(Np->Num) || IS_MINUS(Np->Num)) {
            Np->sign_wrote = !(IS_PLUS(Np->Num) && !IS_MINUS(Np->Num));
        } else {
            Np->sign_wrote = (Np->sign == '+' && IS_FILLMODE(Np->Num) && !IS_LSIGN(Np->Num));
        }
    }

    Np->num_count = Np->Num->post + Np->Num->pre - 1;

    // Handle decimal precision and zero padding
    if (is_to_char && IS_FILLMODE(Np->Num) && IS_DECIMAL(Np->Num)) {
        Np->last_relevant = get_last_relevant_decnum(Np->number);
        // Adjust for zero padding requirements
        if (Np->last_relevant && Np->Num->zero_end > Np->out_pre_spaces) {
            int last_zero_pos = Min(strlen(Np->number) - 1,
                                   Np->Num->zero_end - Np->out_pre_spaces);
            char *last_zero = Np->number + last_zero_pos;
            if (Np->last_relevant < last_zero)
                Np->last_relevant = last_zero;
        }
    }

    // Prepare locale-specific formatting
    NUM_prepare_locale(Np);

    // Set up number pointer
    if (Np->is_to_char)
        Np->number_p = Np->number;
    else
        Np->number_p = Np->number + 1; // first char is space for sign

    // Main pattern processing loop
    for (n = node, Np->inout_p = Np->inout; n->type != NODE_TYPE_END; n++) {
        if (!Np->is_to_char && OVERLOAD_TEST)
            break;

        if (n->type == NODE_TYPE_ACTION) {
            // Process format commands
            switch (n->key->id) {
                case NUM_9:
                case NUM_0:
                case NUM_DEC:
                case NUM_D:
                    if (Np->is_to_char) {
                        NUM_numpart_to_char(Np, n->key->id);
                        continue;
                    } else {
                        NUM_numpart_from_char(Np, n->key->id, input_len);
                        break;
                    }

                case NUM_COMMA:
                    // Handle comma separators
                    if (Np->is_to_char) {
                        *Np->inout_p = (!Np->num_in && IS_FILLMODE(Np->Num)) ?
                                      '\0' : (Np->num_in ? ',' : ' ');
                    } else {
                        if (!Np->num_in && IS_FILLMODE(Np->Num)) continue;
                        if (*Np->inout_p != ',') continue;
                    }
                    break;

                case NUM_G:
                    // Handle locale thousands separator
                    pattern = Np->L_thousands_sep;
                    pattern_len = strlen(pattern);
                    if (Np->is_to_char) {
                        if (!Np->num_in && IS_FILLMODE(Np->Num)) {
                            continue;
                        } else {
                            strcpy(Np->inout_p, pattern);
                            Np->inout_p += pattern_len - 1;
                        }
                    } else {
                        if (!Np->num_in && IS_FILLMODE(Np->Num)) continue;
                        if (AMOUNT_TEST(pattern_len) &&
                            strncmp(Np->inout_p, pattern, pattern_len) == 0)
                            Np->inout_p += pattern_len - 1;
                        else continue;
                    }
                    break;

                case NUM_L:
                    // Handle locale currency symbol
                    pattern = Np->L_currency_symbol;
                    if (Np->is_to_char) {
                        strcpy(Np->inout_p, pattern);
                        Np->inout_p += strlen(pattern) - 1;
                    } else {
                        NUM_eat_non_data_chars(Np, pg_mbstrlen(pattern), input_len);
                        continue;
                    }
                    break;

                case NUM_MI:
                    // Handle minus sign
                    if (Np->is_to_char) {
                        *Np->inout_p = (Np->sign == '-') ? '-' :
                                      (IS_FILLMODE(Np->Num) ? '\0' : ' ');
                    } else {
                        if (*Np->inout_p == '-') {
                            *Np->number = '-';
                        } else {
                            NUM_eat_non_data_chars(Np, 1, input_len);
                            continue;
                        }
                    }
                    break;

                case NUM_PL:
                    // Handle plus sign
                    if (Np->is_to_char) {
                        *Np->inout_p = (Np->sign == '+') ? '+' :
                                      (IS_FILLMODE(Np->Num) ? '\0' : ' ');
                    } else {
                        if (*Np->inout_p == '+') {
                            *Np->number = '+';
                        } else {
                            NUM_eat_non_data_chars(Np, 1, input_len);
                            continue;
                        }
                    }
                    break;

                case NUM_SG:
                    // Handle general sign
                    if (Np->is_to_char) {
                        *Np->inout_p = Np->sign;
                    } else {
                        if (*Np->inout_p == '-' || *Np->inout_p == '+') {
                            *Np->number = *Np->inout_p;
                        } else {
                            NUM_eat_non_data_chars(Np, 1, input_len);
                            continue;
                        }
                    }
                    break;

                default:
                    continue;
            }
        } else {
            // Handle literal characters
            if (Np->is_to_char) {
                strcpy(Np->inout_p, n->character);
                Np->inout_p += strlen(Np->inout_p);
            } else {
                Np->inout_p += pg_mblen(Np->inout_p);
            }
            continue;
        }
        Np->inout_p++;
    }

    // Finalize result
    if (Np->is_to_char) {
        *Np->inout_p = '\0';
        return Np->inout;
    } else {
        // Clean up number string
        if (*(Np->number_p - 1) == '.')
            *(Np->number_p - 1) = '\0';
        else
            *Np->number_p = '\0';

        Np->Num->post = Np->read_post;
        return Np->number;
    }
}
```