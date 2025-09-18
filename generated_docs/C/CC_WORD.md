# CC_WORD

## Location
src/include/regex/regguts.h: 141 - 143

## Overview
CC_WORD is an enumeration constant representing the word character class in PostgreSQL's regular expression engine, used to identify characters that are considered part of words (letters, digits, and underscores).

## Definition


## Detailed Description
CC_WORD is a member of the char_classes enumeration defined in the PostgreSQL regex engine. It represents the word character class, which typically includes alphabetic characters (both uppercase and lowercase), digits (0-9), and the underscore character (_). This constant is used internally by the regex engine to handle the \w escape sequence in regular expressions, which matches any word character. The corresponding negative class \W (non-word characters) also references this constant but with inverted logic.

## Parameters / Member Variables
- This is an enumeration constant, so it has no parameters or member variables
- Value position: Last element in the char_classes enumeration (value 13, since enumeration starts at 0)

## Dependencies
- Functions called/Symbols referenced:
  - Part of char_classes enumeration
- Called from (representative examples):
  - lexescape (in regc_lex.c:699, 703) - handles \w and \W escape sequences
  - cclasscvec (in regc_locale.c:605) - creates character vectors for character classes
  - cclass_column_index (in regc_locale.c:688, 689) - maps character classes to column indices
  - wordchrs (in regcomp.c:2007) - builds word character representations

## Notes and Other Information
- CC_WORD is used in conjunction with the REG_ULOCALE flag to support locale-specific word character definitions
- The regex engine uses this constant to optimize character class matching by pre-computing character vectors
- Total number of character classes is defined as NUM_CCLASSES (14), with CC_WORD being the last one
- This constant enables the PostgreSQL regex engine to support POSIX-style character classes efficiently