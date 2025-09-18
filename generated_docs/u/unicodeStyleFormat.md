# unicodeStyleFormat

## Location
src/fe_utils/print.c: 126 - 138

## Overview
A comprehensive structure that combines all Unicode/UTF-8 table formatting elements and defines the complete styling scheme for table output in PostgreSQL frontend utilities.

## Definition


## Detailed Description
This structure serves as the master container for all Unicode table formatting styles in PostgreSQL's frontend utilities. It aggregates row, column, and border formatting structures along with additional formatting elements for headers, newlines, and text wrapping. The structure supports dual styling options (indicated by the array size of 2) for different formatting contexts and includes comprehensive control over table appearance in terminal output.

## Parameters / Member Variables
- : An array of two unicodeStyleRowFormat structures for different row formatting styles
- : An array of two unicodeStyleColumnFormat structures for different column formatting styles  
- : An array of two unicodeStyleBorderFormat structures for different border formatting styles
- : A pointer to the Unicode character string used for left side of header newlines
- : A pointer to the Unicode character string used for right side of header newlines
- : A pointer to the Unicode character string used for left side of regular newlines
- : A pointer to the Unicode character string used for right side of regular newlines
- : A pointer to the Unicode character string used for left side of wrapped text lines
- : A pointer to the Unicode character string used for right side of wrapped text lines
- : A boolean flag indicating whether to include right border when wrapping text

## Dependencies
- Functions called/Symbols referenced:
  - unicodeStyleRowFormat (at Line 128)
  - unicodeStyleColumnFormat (at Line 129) 
  - unicodeStyleBorderFormat (at Line 130)
- Called from (representative examples):
  - (No direct references found - likely used through variable instantiation)

## Notes and Other Information
- This is the top-level structure that unifies all Unicode table formatting capabilities
- The dual-element arrays suggest support for different formatting modes or line weights
- Provides comprehensive control over table appearance including headers, borders, and text wrapping
- Part of PostgreSQL's sophisticated table formatting system for frontend utilities like psql
- Located in src/fe_utils/print.c as the central formatting structure
- Essential for creating properly formatted Unicode tables with full styling control