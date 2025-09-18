# find_among

## Location
src/backend/snowball/libstemmer/utilities.c: 233 - 297

## Overview
A sophisticated pattern matching function that performs binary search through a sorted array of string patterns, with support for substring matching and callback functions.

## Definition


## Detailed Description
The  function is a core utility in the Snowball stemming framework that performs efficient pattern matching against a sorted array of candidate strings. It uses a binary search algorithm to locate matching patterns in the text starting from the current cursor position.

The function implements a sophisticated matching strategy that handles substring relationships between patterns. When a match is found, it can optionally execute a callback function associated with that pattern. The algorithm optimizes performance by tracking common prefixes during the binary search process, avoiding redundant character comparisons.

Key features include:
- Binary search through sorted pattern arrays for O(log n) performance
- Support for substring patterns through the  field
- Optional callback function execution for complex matching rules  
- Forward cursor advancement on successful matches
- Efficient handling of overlapping or nested pattern relationships

The function returns the  field of the matched pattern, or 0 if no match is found.

## Parameters / Member Variables  
- : Pointer to the Snowball environment structure containing the text buffer and cursor positions
- : Pointer to a sorted array of  structures containing the patterns to match
- : The number of elements in the pattern array

## Dependencies
- Functions called/Symbols referenced:
  -  (structure type for pattern definitions)
  -  (Snowball character type)
- Called from (representative examples):
  - Various language-specific stemming functions across multiple stemmers (Arabic, Catalan, Dutch, English, French, German, Hungarian, Indonesian, Irish, Italian, Portuguese, Romanian, Serbian, Spanish, Tamil, Yiddish)
  - Pattern matching operations in prelude, postlude, and morphological analysis functions
  -  macro in header.h

## Notes and Other Information  
- Critical performance component used extensively throughout all Snowball language stemmers
- The  array must be pre-sorted for the binary search to work correctly
- Supports complex pattern hierarchies through the  mechanism
- Callback functions enable context-sensitive matching rules
- Advances cursor position only on successful matches with proper pattern length
- Part of the forward-matching family of functions in Snowball utilities
- The algorithm handles edge cases like single-element arrays and boundary conditions efficiently