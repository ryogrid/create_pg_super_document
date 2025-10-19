# NUM_numpart_to_char

## Location
[src/backend/utils/adt/formatting.c:5620-5809](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L5620-L5809)

## Overview
Formats and writes numeric parts (digits, signs, decimal points) to output strings during TO_CHAR() processing, handling various formatting modes and locale-specific conventions.

## Definition

```c
static void
NUM_numpart_to_char(NUMProc *Np, int id)
```
## Detailed Description
This function is the counterpart to NUM_numpart_from_char, responsible for generating formatted numeric output during TO_CHAR() operations. It handles the complex logic of positioning signs, digits, and decimal points according to PostgreSQL's formatting specifications.

Key responsibilities include:
- Sign placement (pre-sign, post-sign, bracket notation)
- Digit output with proper spacing and zero handling
- Decimal point positioning with locale awareness
- Fill mode (FM) behavior that suppresses leading/trailing spaces
- Roman numeral detection (early return)
- Zero padding and suppression logic
- Handling of various format patterns (NUM_9, NUM_0, NUM_D, NUM_DEC)

The function maintains careful state management to ensure signs are written exactly once at the appropriate position, and coordinates with NUM_processor to generate properly formatted numeric strings.

## Parameters / Member Variables
- : Pointer to NUMProc structure containing formatting state and configuration
  - : Current position in output buffer
  - : Current position in input number string
  - : Flag tracking whether sign has been output
  - : Current position in format pattern
  - : Flag indicating if numeric content has been written
  - : Pointer to last significant digit for FM mode
  - : Sign character ('+', '-', ' ')
- uid=1000(ryo) gid=1000(ryo) groups=1000(ryo),4(adm),20(dialout),24(cdrom),25(floppy),27(sudo),29(audio),30(dip),44(video),46(plugdev),117(netdev),998(ollama),999(docker): Format token identifier (NUM_9, NUM_0, NUM_D, NUM_DEC, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - IS_ROMAN, IS_ZERO, IS_LSIGN, IS_BRACKET, IS_FILLMODE, IS_PREDEC_SPACE, IS_DECIMAL (format flag checking macros)
  - NUM_LSIGN_PRE, NUM_LSIGN_POST (locale sign position constants)
  - strcpy, strlen (string operations)
  - elog (debug logging)
- Called from (representative examples):
  - [NUM_processor](NUM_processor.md) (formatting.c:6035)
  - DCH_ZONED (formatting.c:1080)

## Notes and Other Information
- Central component of TO_CHAR() numeric formatting with complex conditional logic
- Early returns for Roman numeral formatting to avoid conflicts
- Implements PostgreSQL's specific spacing and zero-handling rules
- Handles edge cases like "9.9" → " .1" for predecimal spaces
- Manages both locale-specific and simple sign conventions
- Supports bracket notation where negative numbers appear as <123>
- Fill mode (FM) suppresses unnecessary spaces and trailing zeros
- Coordinates sign placement timing to avoid duplication
- Debug logging available when DEBUG_TO_FROM_CHAR is enabled
- Must handle the complex interaction between zero padding, fill mode, and decimal positioning

## Simplified Source

```c
static void
NUM_numpart_to_char(NUMProc *Np, int id)
{
    // Skip Roman numeral processing
    if (IS_ROMAN(Np->Num))
        return;

    Np->num_in = false;

    // Write sign if appropriate (once per number, at correct position)
    if (!Np->sign_wrote &&
        (Np->num_curr >= Np->out_pre_spaces ||
         (IS_ZERO(Np->Num) && Np->Num->zero_start == Np->num_curr)) &&
        (!IS_PREDEC_SPACE(Np) || (Np->last_relevant && *Np->last_relevant == '.')))
    {
        if (IS_LSIGN(Np->Num) && Np->Num->lsign == NUM_LSIGN_PRE)
        {
            // Locale-specific pre-sign
            if (Np->sign == '-')
                strcpy(Np->inout_p, Np->L_negative_sign);
            else
                strcpy(Np->inout_p, Np->L_positive_sign);
            Np->inout_p += strlen(Np->inout_p);
            Np->sign_wrote = true;
        }
        else if (IS_BRACKET(Np->Num))
        {
            // Bracket notation: positive=' ', negative='<'
            *Np->inout_p = (Np->sign == '+') ? ' ' : '<';
            ++Np->inout_p;
            Np->sign_wrote = true;
        }
        else if (Np->sign == '+')
        {
            // Positive sign (space in non-fill mode)
            if (!IS_FILLMODE(Np->Num))
            {
                *Np->inout_p = ' ';
                ++Np->inout_p;
            }
            Np->sign_wrote = true;
        }
        else if (Np->sign == '-')
        {
            // Negative sign
            *Np->inout_p = '-';
            ++Np->inout_p;
            Np->sign_wrote = true;
        }
    }

    // Process digits, decimal points, and spacing
    if (id == NUM_9 || id == NUM_0 || id == NUM_D || id == NUM_DEC)
    {
        if (Np->num_curr < Np->out_pre_spaces &&
            (Np->Num->zero_start > Np->num_curr || !IS_ZERO(Np->Num)))
        {
            // Write leading space (unless in fill mode)
            if (!IS_FILLMODE(Np->Num))
            {
                *Np->inout_p = ' ';
                ++Np->inout_p;
            }
        }
        else if (IS_ZERO(Np->Num) &&
                 Np->num_curr < Np->out_pre_spaces &&
                 Np->Num->zero_start <= Np->num_curr)
        {
            // Write leading zero
            *Np->inout_p = '0';
            ++Np->inout_p;
            Np->num_in = true;
        }
        else
        {
            // Process decimal point or digit
            if (*Np->number_p == '.')
            {
                // Write decimal point if needed
                if (!Np->last_relevant || *Np->last_relevant != '.' ||
                    (IS_FILLMODE(Np->Num) && Np->last_relevant && *Np->last_relevant == '.'))
                {
                    strcpy(Np->inout_p, Np->decimal);
                    Np->inout_p += strlen(Np->inout_p);
                }
            }
            else
            {
                // Write digit or handle special spacing
                if (Np->last_relevant && Np->number_p > Np->last_relevant && id != NUM_0)
                {
                    // Skip trailing insignificant digits
                }
                else if (IS_PREDEC_SPACE(Np))
                {
                    // Handle predecimal spacing: "0.1" -> " .1"
                    if (!IS_FILLMODE(Np->Num))
                    {
                        *Np->inout_p = ' ';
                        ++Np->inout_p;
                    }
                    else if (Np->last_relevant && *Np->last_relevant == '.')
                    {
                        *Np->inout_p = '0';
                        ++Np->inout_p;
                    }
                }
                else
                {
                    // Write the actual digit
                    *Np->inout_p = *Np->number_p;
                    ++Np->inout_p;
                    Np->num_in = true;
                }
            }

            // Advance number pointer if valid
            if (*Np->number_p)
                ++Np->number_p;
        }

        // Handle end-of-number post-signs
        int end = Np->num_count + (Np->out_pre_spaces ? 1 : 0) + (IS_DECIMAL(Np->Num) ? 1 : 0);
        if (Np->last_relevant && Np->last_relevant == Np->number_p)
            end = Np->num_curr;

        if (Np->num_curr + 1 == end)
        {
            if (Np->sign_wrote && IS_BRACKET(Np->Num))
            {
                // Close bracket: positive=' ', negative='>'
                *Np->inout_p = (Np->sign == '+') ? ' ' : '>';
                ++Np->inout_p;
            }
            else if (IS_LSIGN(Np->Num) && Np->Num->lsign == NUM_LSIGN_POST)
            {
                // Locale-specific post-sign
                if (Np->sign == '-')
                    strcpy(Np->inout_p, Np->L_negative_sign);
                else
                    strcpy(Np->inout_p, Np->L_positive_sign);
                Np->inout_p += strlen(Np->inout_p);
            }
        }
    }

    ++Np->num_curr;
}
```