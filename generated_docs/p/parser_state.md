# parser_state

## Location
src/tools/pg_bsd_indent/indent_globs.h: 234 - 339

## Overview
A comprehensive structure that maintains the complete parsing and formatting state for the pg_bsd_indent tool, tracking indentation levels, brace nesting, comment handling, and all formatting context.

## Definition


## Detailed Description
The parser_state structure is the central data structure of the pg_bsd_indent tool, maintaining all necessary state information for parsing and formatting C source code. This structure tracks multiple aspects of code formatting simultaneously: indentation management through various level counters, comment handling with specialized positioning logic, declaration and statement context tracking, and parenthesis/brace nesting management.

The structure uses several stacks (p_stack, il, cstk, paren_indents) to handle nested constructs properly, ensuring that formatting decisions can account for the complete syntactic context. The numerous boolean flags track parsing state transitions that affect formatting rules, such as being inside declarations, statements, or special constructs like switch-case blocks.

## Parameters / Member Variables
- : The most recently processed token type
- : Main parser stack for tracking syntactic context
- : Stack storing indentation levels for nested constructs
- : Stack for case statement indentation levels
- : Flag indicating when inside a "boxed" comment block
- : Indentation offset for subsequent lines of boxed comments
- : Column count before box comment start for proper alignment
- : Bitmask tracking close parentheses that might end cast expressions
- : Bitmask for close parentheses that definitely don't end casts
- : Flag set when inside a block initialization
- : Nesting level of braces within initializations
- : Flag indicating if last scanned character was a newline
- : Flag set when declarator seen without left parenthesis
- : Flag set by dump_line when current line is blank
- : Flag indicating if last token started in column 1
- : Target column position for current comment
- : Standard column for comments to the right of code
- : Count of lines containing comments
- : Current nesting depth for structures or initializations
- : Column position for comments following declarations
- : Flag indicating declaration content on current line
- : Target indentation level for the next line
- : Flag set when processing declaration statements
- : Flag set when inside a statement
- : Current base indentation level
- : Number of spaces/tabs per indentation level
- : Extra indentation when in middle of statement
- : Flag set after tokens that force following operators to be unary
- : Flag to prevent breaking declarations after commas
- : Flag for left-justifying declarations
- : Count of processed comments
- : Count of output lines written
- : Indentation level for following statement
- : Current parenthesization depth
- : Array storing column positions for each parenthesis level
- : Flag set when current line label is a case statement
- : Flag to buffer content until statement start after if/while/etc
- : Left displacement for comments not to the right of code
- : Flag to terminate current line with form feed
- : Flag indicating next token should be prefixed with blank
- : Flag for special handling of else-if pairs
- : Column position for declared identifiers
- : Like decl_indent but for local declarations
- : Type identifier for keywords (0 if not keyword)
- : Tracking variable for declaration indentation
- : Distance to indent case labels from switch statement
- : Flag when processing parameter declarations
- : Flag for parameter indentation behavior
- : Top-of-stack pointer for parser stack operations
- : Name of current procedure being processed
- : Flag indicating recent declaration processing

## Dependencies
- Functions called/Symbols referenced:
  - DECLARE_INDENT_GLOBALS (macro for global declarations)
- Referenced by (representative examples):
  - [main](../m/main.md) function in indent.c:87
  - nitems macro usage in indent.h:33,41
  - [lexi](../l/lexi.md) function in lexi.c:216

## Notes and Other Information
- This structure is the heart of the pg_bsd_indent state machine, containing all context needed for proper C code formatting
- Multiple stack structures (p_stack, il, cstk) handle nested language constructs
- The large number of boolean flags reflects the complexity of C syntax and formatting rules
- Array sizes (256 for p_stack, 64 for il, 32 for cstk, 20 for paren_indents) represent practical limits for nested constructs
- The procname field suggests procedure-aware formatting capabilities
- Float types for case_indent and cstk allow fractional indentation values for fine-grained positioning