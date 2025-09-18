# printTextLineWrap

## Location
src/include/fe_utils/print.h: 67 - 68

## Overview
printTextLineWrap is an enumeration type that defines different line wrapping conditions used when formatting text output in PostgreSQL's frontend printing utilities.

## Definition

(Defined in src/include/fe_utils/print.h:61-67)

## Detailed Description
The printTextLineWrap enumeration categorizes different scenarios that can cause line wrapping or line breaks when displaying formatted text output. This enumeration helps the printing system distinguish between different types of line breaks: those caused by content exceeding display width limits, those explicitly present in the source data, and cases where no wrapping occurs. This distinction is important for proper text formatting and alignment in terminal-based table displays.

## Parameters / Member Variables
- : No line wrapping is applied; the line fits within the available space
- : Line wrapping occurs due to content being too long for the available display width
- : Line break is caused by an explicit newline character present in the source data

## Dependencies
- Functions called/Symbols referenced:
  - None (enumeration type definition)
- Called from (representative examples):
  - print_aligned_text (src/fe_utils/print.c:668)

## Notes and Other Information
- This enumeration is primarily used in text alignment and formatting functions to handle different line breaking scenarios appropriately
- Distinguishing between wraparound and explicit newlines is crucial for maintaining proper text formatting and alignment
- Used in conjunction with other printing enumerations and structures to provide comprehensive text formatting control
- Essential for implementing proper text wrapping behavior in PostgreSQL's command-line interface tools