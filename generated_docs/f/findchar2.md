# findchar2

## Location
src/backend/tsearch/spell.c: 242 - 256

## Overview
A multibyte-aware character search function that locates the first occurrence of either of two specified ASCII characters in a string, used in PostgreSQL's affix file parsing for case-insensitive searches.

## Definition


## Detailed Description
The  function extends the functionality of  by searching for either of two target characters in a string. This function is particularly useful for case-insensitive searches where you need to find either the lowercase or uppercase version of a character. Like , it properly handles multibyte character encodings by using  to advance through the string and  for character comparison.

The function iterates through the input string character by character, checking at each position whether the current character matches either of the two target characters. This is especially useful in affix file parsing where directive keywords may appear in different cases, and you need to locate specific flags regardless of their case.

The primary use case is in parsing Hunspell affix files, specifically for finding case-insensitive flags in compound word directives where 'l' or 'L' flags indicate compound word settings.

## Parameters / Member Variables
- : Pointer to the null-terminated string to search in
- : The first ASCII character to search for (must be a plain ASCII character)
- : The second ASCII character to search for (must be a plain ASCII character)

## Dependencies
- Functions called/Symbols referenced:
  - t_iseq (macro for character comparison, defined in ts_locale.h)
  - pg_mblen (function for getting multibyte character length)
  - TOUCHAR (macro used internally by t_iseq)
- Called from:
  - NIImportAffixes (src/backend/tsearch/spell.c:1464) - for case-insensitive flag parsing

## Notes and Other Information
- This is a static function, accessible only within the spell.c compilation unit
- Returns a pointer to the first occurrence of either character, or NULL if neither is found
- Both character parameters must be plain ASCII characters (as required by t_iseq macro)
- Handles multibyte encodings correctly by advancing through the string using pg_mblen()
- Commonly used for case-insensitive character searches, particularly finding 'l' or 'L' flags
- More efficient than calling findchar() twice for two different characters
- Used specifically in affix file parsing for compound word directive processing
- Part of PostgreSQL's text search infrastructure for processing Hunspell affix files