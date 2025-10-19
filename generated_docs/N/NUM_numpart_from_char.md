# NUM_numpart_from_char

## Location
[src/backend/utils/adt/formatting.c:5405-5608](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L5405-L5608)

## Overview
Extracts numeric parts (digits, signs, decimal points) from input strings during TO_NUMBER() processing, handling locale-specific formatting and various sign conventions.

## Definition

```c
static void
NUM_numpart_from_char(NUMProc *Np, int id, int input_len)
```
## Detailed Description
This function is a core component of PostgreSQL's TO_NUMBER() functionality, responsible for parsing and extracting numeric components from formatted input strings. It handles complex parsing scenarios including:

- Pre-number sign detection (both locale-specific and simple +/- signs)
- Digit extraction with position tracking (pre/post decimal)
- Decimal point recognition (locale-aware)  
- Post-number sign detection for various formatting patterns
- Bracket notation for negative numbers (< >)
- Fill mode (FM) and exact positioning requirements

The function processes input character by character, maintaining state about what has been read and updating the numeric buffer accordingly. It includes extensive debug logging and boundary checking to handle various edge cases in number parsing.

## Parameters / Member Variables
- : Pointer to NUMProc structure containing parsing state and configuration
  - : Current position in input string
  - : Buffer for constructing parsed number
  - : Current position in number buffer
  - : Count of digits read before decimal point
  - : Count of digits read after decimal point
  - : Flag indicating decimal point has been encountered
- uid=1000(ryo) gid=1000(ryo) groups=1000(ryo),4(adm),20(dialout),24(cdrom),25(floppy),27(sudo),29(audio),30(dip),44(video),46(plugdev),117(netdev),998(ollama),999(docker): Format token identifier (NUM_0, NUM_9, NUM_DEC, etc.)
- : Total length of input string for boundary checking

## Dependencies
- Functions called/Symbols referenced:
  - OVERLOAD_TEST (boundary checking macro)
  - IS_LSIGN, IS_DECIMAL, IS_BRACKET, IS_PLUS, IS_MINUS (format flag checking macros)
  - AMOUNT_TEST (input length validation macro)
  - strlen, strncmp (string operations)
  - isdigit (character classification)
  - elog (debug logging)
- Called from (representative examples):
  - [NUM_processor](NUM_processor.md) (formatting.c:6040)
  - DCH_ZONED (formatting.c:1079)

## Notes and Other Information
- Critical component of TO_NUMBER() parsing with complex state management
- Handles both pre-sign and post-sign scenarios based on format requirements  
- Supports locale-specific signs, decimal points, and thousands separators
- Includes extensive boundary checking to prevent buffer overflows
- Debug logging available when DEBUG_TO_FROM_CHAR is enabled
- Must handle various ambiguous sign positioning scenarios in fill mode (FM)
- Supports bracket notation where '<' represents negative sign
- Carefully manages input position to coordinate with NUM_processor() main loop

## Simplified Source

```c
static void
NUM_numpart_from_char(NUMProc *Np, int id, int input_len)
{
    bool isread = false;

    // Skip boundary checks for simplification
    if (*Np->inout_p == ' ')
        Np->inout_p++;

    // Read sign before number (only if no digits read yet)
    if (*Np->number == ' ' && (id == NUM_0 || id == NUM_9) &&
        (Np->read_pre + Np->read_post) == 0)
    {
        if (IS_LSIGN(Np->Num) && Np->Num->lsign == NUM_LSIGN_PRE)
        {
            // Try to match locale-specific negative/positive signs
            int x = strlen(Np->L_negative_sign);
            if (x && strncmp(Np->inout_p, Np->L_negative_sign, x) == 0)
            {
                Np->inout_p += x;
                *Np->number = '-';
            }
            else if ((x = strlen(Np->L_positive_sign)) &&
                     strncmp(Np->inout_p, Np->L_positive_sign, x) == 0)
            {
                Np->inout_p += x;
                *Np->number = '+';
            }
        }
        else
        {
            // Handle simple +/- or bracket notation
            if (*Np->inout_p == '-' ||
                (IS_BRACKET(Np->Num) && *Np->inout_p == '<'))
            {
                *Np->number = '-';
                Np->inout_p++;
            }
            else if (*Np->inout_p == '+')
            {
                *Np->number = '+';
                Np->inout_p++;
            }
        }
    }

    // Read digit or decimal point
    if (isdigit((unsigned char) *Np->inout_p))
    {
        // Stop if we've read all post-decimal digits allowed
        if (Np->read_dec && Np->read_post == Np->Num->post)
            return;

        // Copy digit to number buffer
        *Np->number_p = *Np->inout_p;
        Np->number_p++;

        // Update digit counters
        if (Np->read_dec)
            Np->read_post++;
        else
            Np->read_pre++;

        isread = true;
    }
    else if (IS_DECIMAL(Np->Num) && !Np->read_dec)
    {
        // Try to read decimal point
        int x = strlen(Np->decimal);
        if (x && strncmp(Np->inout_p, Np->decimal, x) == 0)
        {
            Np->inout_p += x - 1;
            *Np->number_p = '.';
            Np->number_p++;
            Np->read_dec = true;
            isread = true;
        }
    }

    // Read post-number sign if we have digits and no sign yet
    if (*Np->number == ' ' && Np->read_pre + Np->read_post > 0)
    {
        if (IS_LSIGN(Np->Num) && isread &&
            (Np->inout_p + 1) < Np->inout + input_len &&
            !isdigit((unsigned char) *(Np->inout_p + 1)))
        {
            // Try locale post-sign
            int x;
            char *tmp = Np->inout_p++;

            if ((x = strlen(Np->L_negative_sign)) &&
                strncmp(Np->inout_p, Np->L_negative_sign, x) == 0)
            {
                Np->inout_p += x - 1;
                *Np->number = '-';
            }
            else if ((x = strlen(Np->L_positive_sign)) &&
                     strncmp(Np->inout_p, Np->L_positive_sign, x) == 0)
            {
                Np->inout_p += x - 1;
                *Np->number = '+';
            }

            if (*Np->number == ' ')
                Np->inout_p = tmp;  // Reset if no sign found
        }
        else if (!isread && !IS_LSIGN(Np->Num) &&
                 (IS_PLUS(Np->Num) || IS_MINUS(Np->Num)))
        {
            // Simple post-sign for non-locale formats
            if (*Np->inout_p == '-' || *Np->inout_p == '+')
                *Np->number = *Np->inout_p;
        }
    }
}
```