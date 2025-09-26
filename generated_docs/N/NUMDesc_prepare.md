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
  - FormatNode, NUMDesc (struct types)
  - Various IS_* macros (IS_DECIMAL, IS_BRACKET, IS_MULTI, etc.)
  - NUM_* constants for pattern identification
  - NUM_F_* flag constants
  - NUM_LSIGN_* constants for sign positioning
  - ereport for error reporting
- Called from (representative examples):
  - DCH_ZONED
  - parse_format

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