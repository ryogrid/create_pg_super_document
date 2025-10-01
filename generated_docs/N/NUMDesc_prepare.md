# NUMDesc_prepare

## Location
[src/backend/utils/adt/formatting.c:1153-1327](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L1153-L1327)

## Overview
Prepares and validates a NUMDesc (number description) structure by processing format nodes and setting appropriate flags and counters based on numeric formatting patterns.

## Definition
```c
static void
NUMDesc_prepare(NUMDesc *num, FormatNode *n)
```

## Detailed Description
This function processes FormatNode structures containing numeric formatting patterns and updates the NUMDesc structure accordingly. It handles validation of format pattern combinations, sets formatting flags, and maintains counters for different parts of the number format (pre-decimal, post-decimal, zero padding, etc.). The function implements comprehensive error checking to ensure incompatible format patterns are not combined.

Key responsibilities:
- Validates format pattern compatibility and reports syntax errors for invalid combinations
- Sets formatting flags (NUM_F_*) based on encountered patterns
- Maintains counters for digits before/after decimal point
- Handles special formatting modes like Roman numerals, scientific notation, and sign handling
- Manages locale-specific formatting requirements

The function processes various numeric format patterns:
- Digit patterns: '9', '0' for number positioning and zero padding
- Decimal handling: 'D', 'DEC' for decimal points
- Sign handling: 'S', 'MI', 'PL', 'SG', 'PR' for positive/negative indicators
- Special formats: Roman numerals (RN/rn), scientific notation (EEEE), multipliers (V)
- Formatting modes: Fill mode (FM), blank handling (B)
- Locale patterns: 'L', 'G' for currency and grouping

## Parameters / Member Variables
- `num`: Pointer to NUMDesc structure to be prepared and configured
- `n`: Pointer to FormatNode containing the format pattern to process

## Dependencies
- Functions called/Symbols referenced:
  - [FormatNode](../F/FormatNode.md), NUMDesc (struct types)
  - Various IS_* macros (IS_DECIMAL, IS_BRACKET, IS_MULTI, etc.)
  - NUM_* constants for pattern identification
  - NUM_F_* flag constants
  - NUM_LSIGN_* constants for sign positioning
  - ereport for error reporting
- Called from (representative examples):
  - DCH_ZONED
  - [parse_format](../p/parse_format.md)

## Notes and Other Information
- This is a static function, only accessible within formatting.c
- Performs extensive validation to prevent incompatible format combinations
- Key validation rules include:
  - EEEE must be the last pattern and is incompatible with most other formats
  - Cannot mix 'V' (multiplier) with decimal points
  - Cannot use multiple sign indicators simultaneously
  - Digits ('9', '0') must appear before brackets ('PR')
  - Cannot have multiple decimal points
- Sets need_locale flag when locale-specific formatting is required
- Manages complex state tracking for sign positioning (pre vs post decimal)

## Simplified Source

```c
static void
NUMDesc_prepare(NUMDesc *num, FormatNode *n)
{
    if (n->type != NODE_TYPE_ACTION)
        return;

    // Validate EEEE placement - must be last
    if (IS_EEEE(num) && n->key->id != NUM_E)
        ereport(ERROR, "EEEE must be the last pattern used");

    switch (n->key->id) {
        case NUM_9:    // Digit placeholder
            // Validate placement and update counters
            if (IS_BRACKET(num)) ereport(ERROR, "9 must be ahead of PR");
            if (IS_MULTI(num)) ++num->multi;
            else if (IS_DECIMAL(num)) ++num->post;
            else ++num->pre;
            break;

        case NUM_0:    // Zero-padded digit
            // Similar to NUM_9 but enables zero padding
            if (IS_BRACKET(num)) ereport(ERROR, "0 must be ahead of PR");
            if (!IS_ZERO(num) && !IS_DECIMAL(num)) {
                num->flag |= NUM_F_ZERO;
                num->zero_start = num->pre + 1;
            }
            if (!IS_DECIMAL(num)) ++num->pre;
            else ++num->post;
            num->zero_end = num->pre + num->post;
            break;

        case NUM_D:    // Locale decimal point
            num->flag |= NUM_F_LDECIMAL;
            num->need_locale = true;
            // FALLTHROUGH
        case NUM_DEC:  // Regular decimal point
            if (IS_DECIMAL(num)) ereport(ERROR, "multiple decimal points");
            if (IS_MULTI(num)) ereport(ERROR, "cannot use V and decimal point together");
            num->flag |= NUM_F_DECIMAL;
            break;

        case NUM_FM:   // Fill mode
            num->flag |= NUM_F_FILLMODE;
            break;

        case NUM_S:    // Sign indicator
            // Complex sign validation and positioning logic
            if (IS_LSIGN(num)) ereport(ERROR, "cannot use S twice");
            if (IS_PLUS(num) || IS_MINUS(num) || IS_BRACKET(num))
                ereport(ERROR, "cannot use S and PL/MI/SG/PR together");

            if (!IS_DECIMAL(num)) {
                num->lsign = NUM_LSIGN_PRE;
                num->pre_lsign_num = num->pre;
            } else if (num->lsign == NUM_LSIGN_NONE) {
                num->lsign = NUM_LSIGN_POST;
            }
            num->need_locale = true;
            num->flag |= NUM_F_LSIGN;
            break;

        case NUM_MI:   // Minus sign
        case NUM_PL:   // Plus sign
        case NUM_SG:   // Sign (both + and -)
        case NUM_PR:   // Brackets for negatives
            // Handle various sign indicators with validation
            // Each has specific compatibility rules
            break;

        case NUM_E:    // Scientific notation
            if (IS_EEEE(num)) ereport(ERROR, "cannot use EEEE twice");
            // Validate EEEE compatibility with other formats
            if (IS_BLANK(num) || IS_FILLMODE(num) || /* other incompatible flags */)
                ereport(ERROR, "EEEE is incompatible with other formats");
            num->flag |= NUM_F_EEEE;
            break;

        // Additional cases: NUM_B, NUM_rn/RN, NUM_L/G, NUM_V
        // Each sets appropriate flags and performs validation
    }
}
```