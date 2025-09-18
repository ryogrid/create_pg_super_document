# is_space

## Location
src/timezone/zic.c: 3468 - 3485

## Overview
The is_space function determines whether a character is a whitespace character according to C locale standards, providing a portable whitespace detection utility.

## Definition


## Detailed Description
The is_space function provides a locale-independent implementation for detecting whitespace characters within PostgreSQL's timezone compiler. Unlike the standard library's isspace() function which can vary based on locale settings, this function explicitly defines whitespace according to the C locale specification. It uses a switch statement to efficiently check for the six standard whitespace characters: space, form feed, newline, carriage return, tab, and vertical tab. This ensures consistent parsing behavior regardless of the system's locale configuration.

## Parameters / Member Variables
- : A char value representing the character to test for whitespace properties

## Dependencies
- Functions called/Symbols referenced:
  - None (self-contained function)
- Called from (representative examples):
  - getfields (at lines 3729, 3747, 3748)

## Notes and Other Information
- Returns true for the six C locale whitespace characters: ' ', '\f', '\n', '\r', '\t', '\v'
- Provides locale-independent behavior unlike standard library isspace()
- Uses efficient switch statement for character comparison
- Essential for consistent text parsing in timezone data files
- Helps ensure portable behavior across different system locales and character encodings
- Part of the timezone compiler's text processing infrastructure for parsing timezone rule files