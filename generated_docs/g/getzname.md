# getzname

## Location
src/timezone/localtime.c: 642 - 662

## Overview
The  function scans a timezone string to find the end of a timezone abbreviation by advancing until it encounters a character that is invalid for timezone names.

## Definition


## Detailed Description
This static function parses timezone strings by scanning forward from a given position until it finds a character that terminates a timezone abbreviation. It stops when encountering digits, commas, hyphens, plus signs, or null terminators - all characters that typically mark the end of a timezone name and the beginning of other timezone rule components like offsets or transition rules.

## Parameters / Member Variables
- : Pointer to a position within a timezone string to begin scanning from

## Dependencies
- Functions called/Symbols referenced:
  - is_digit (macro/function for digit checking)
- Called from (representative examples):
  - tzparse

## Notes and Other Information
- Returns a pointer to the first character that is not valid in a timezone abbreviation
- Valid timezone abbreviation characters are any characters except: digits (0-9), comma (,), hyphen (-), plus (+), and null terminator (\0)
- Used during timezone rule parsing to extract timezone abbreviation names from POSIX timezone strings
- Part of the timezone string parsing infrastructure in PostgreSQL's timezone system
- The function advances the pointer until it finds a delimiter or terminator character