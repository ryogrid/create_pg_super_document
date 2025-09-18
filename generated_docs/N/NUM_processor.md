# NUM_processor

## Location
[src/backend/utils/adt/formatting.c:5823-6306](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L5823-L6306)

## Overview
The core formatting engine that processes number formatting patterns and converts between numeric values and their textual representations in PostgreSQL's format system.

## Definition


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
- : Array of FormatNode structures defining the format pattern
- : NUMDesc structure containing format specifications and flags
- : Input/output buffer for the formatted string
- : Numeric string to be processed
- : Length of input buffer for boundary checking
- : Number of leading spaces in TO_CHAR output
- : Sign character for the number ('+', '-', or space)
- : Boolean indicating direction (true for TO_CHAR, false for TO_NUMBER)
- : Collation identifier for locale-specific formatting

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