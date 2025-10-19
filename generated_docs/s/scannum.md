# scannum

## Location
[src/backend/regex/regcomp.c:1555-1585](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regcomp.c#L1555-L1585)

## Overview
The scannum function scans and parses a numeric value from the regular expression input stream, used primarily for parsing repetition counts in quantifiers like {n,m}.

## Definition
```c
static int                      /* value, <= DUPMAX */
scannum(struct vars *v)
```

## Detailed Description
The scannum function is part of PostgreSQL's regular expression parser implementation. It reads consecutive digit characters from the input stream and converts them into an integer value. This function is primarily used for parsing numeric quantifiers in regular expressions, such as the numbers in {3}, {2,5}, or {1,10} patterns.

The function operates with built-in bounds checking:
1. It accumulates digits while the current character is a digit and the result is less than DUPMAX
2. It performs decimal conversion (n = n * 10 + digit_value)
3. If it encounters more digits after reaching DUPMAX, or if the final value exceeds DUPMAX, it reports a REG_BADBR error
4. It returns the parsed numeric value or 0 on error

This ensures that repetition counts stay within reasonable bounds and prevents potential overflow issues.

## Parameters / Member Variables
- `v`: Pointer to vars structure containing regex parsing context, including the current input position and nextvalue (current character's numeric value)

## Dependencies
- Functions called/Symbols referenced:
  - SEE (macro for checking current character type)
  - DIGIT (character class constant)
  - DUPMAX (maximum duplication count constant)
  - NEXT (macro for advancing to next character)
  - ERR (macro for setting error state)
  - REG_BADBR (error code for bad brace/quantifier)
- Called from (representative examples):
  - ARCV (multiple call sites in regcomp.c for parsing quantifier bounds)

## Notes and Other Information
- This is a static function internal to the regex compilation module
- Returns values are bounded by DUPMAX to prevent overflow and unreasonable repetition counts
- The function handles error cases by setting REG_BADBR error and returning 0
- Used specifically for parsing the numeric components of quantifiers like {n}, {n,}, and {n,m}
- The function assumes that v->nextvalue contains the numeric value of the current digit character
- Bounds checking prevents both integer overflow and excessively large repetition counts

## Simplified Source

```c
static int scannum(struct vars *v) {
    int n = 0;

    // Parse digits while within bounds
    while (SEE(DIGIT) && n < DUPMAX) {
        n = n * 10 + v->nextvalue;  // Convert digit to number
        NEXT();  // Move to next character
    }

    // Check for overflow or too many digits
    if (SEE(DIGIT) || n > DUPMAX) {
        ERR(REG_BADBR);  // Bad brace/quantifier error
        return 0;
    }

    return n;  // Return parsed number
}
```