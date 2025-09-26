# pg_mule_dsplen

## Location
src/common/wchar.c: 811 - 838

## Overview
Calculates the display length (number of screen columns) for a MULE encoded character, approximating multibyte characters as double-width.

## Definition
static int pg_mule_dsplen(const unsigned char *s)

## Detailed Description
This function determines the display width of a character in the MULE (Multi-Lingual Emacs) encoding system. MULE is a character encoding scheme that supports multiple languages by using leading codes to distinguish character sets. The function approximates the display width based on the character's leading code category, assuming that multibyte characters are double-wide on screen, which is a reasonable approximation for the MULE charsets supported by PostgreSQL.

## Parameters / Member Variables
- : Pointer to the unsigned character to analyze for display length calculation

## Dependencies
- Functions called/Symbols referenced:
  - IS_LC1 (macro for checking Latin Character set 1)
  - IS_LCPRV1 (macro for checking Latin Character Private set 1)
  - IS_LC2 (macro for checking Latin Character set 2)
  - IS_LCPRV2 (macro for checking Latin Character Private set 2)
- Called from (representative examples):
  - pg_encoding_set_invalid

## Notes and Other Information
- Returns 1 for single-byte characters (LC1, LCPRV1, or ASCII)
- Returns 2 for double-byte characters (LC2, LCPRV2)
- The approximation of all multibyte charsets being double-wide is acknowledged as not entirely accurate but acceptable for MULE charsets
- This is a static function internal to the character conversion system