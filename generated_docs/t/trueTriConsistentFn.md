# trueTriConsistentFn

## Location
src/backend/access/gin/ginlogic.c: 56 - 64

## Overview
A dummy ternary consistent function for GIN index EVERYTHING keys that always returns GIN_TRUE, indicating definitive match for any query.

## Definition


## Detailed Description
This function serves as a placeholder ternary consistent function for GIN (Generalized Inverted Index) scan keys representing "EVERYTHING" keys. Unlike the boolean version (trueConsistentFn), this function returns a ternary value (GIN_TRUE) rather than a simple boolean. It always indicates a definitive positive match without performing any actual consistency checking, making it suitable for scan keys that should match all possible values.

## Parameters / Member Variables
- : A GinScanKey pointer representing the scan key being processed. This parameter is not used in the function body since the result is always GIN_TRUE.

## Dependencies
- Functions called/Symbols referenced:
  - GinScanKey (struct type)
  - GIN_TRUE (enum value from GinTernaryValue)
- Called from (representative examples):
  - ginInitConsistentFunction

## Notes and Other Information
- This is a static function, only visible within the ginlogic.c compilation unit
- Part of the GIN indexing infrastructure in PostgreSQL
- Returns GinTernaryValue which allows for three states: GIN_FALSE, GIN_MAYBE, and GIN_TRUE
- Used specifically for EVERYTHING keys where no actual consistency checking is needed
- Companion function to trueConsistentFn, but for ternary logic scenarios
- Located in src/backend/access/gin/ginlogic.c:56-64