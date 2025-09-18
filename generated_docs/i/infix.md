# infix

## Location
src/backend/utils/adt/tsquery.c: 991 - 1145

## Overview
The  function recursively traverses a TSQuery tree structure and converts it into human-readable infix notation string representation, handling operator precedence and parentheses.

## Definition


## Detailed Description
This function is a core component of PostgreSQL's TSQuery output formatting system. It performs a recursive tree traversal of the internal TSQuery representation (stored in prefix/polish notation) and converts it back to the familiar infix notation that users expect to see. The function handles three main types of query items:

1. **Query Values (QI_VAL)**: Formats operand terms with proper quoting, escaping, weight annotations (:A, :B, :C, :D), and prefix indicators (:*)
2. **NOT operators (OP_NOT)**: Handles unary negation with appropriate precedence and parentheses
3. **Binary operators**: Manages AND (&), OR (|), and PHRASE (<->, <N>) operators with proper precedence rules

The function implements sophisticated precedence handling to minimize unnecessary parentheses while maintaining semantic correctness. It also includes special logic for phrase operators since they are order-dependent and require careful parenthesization.

The output buffer is dynamically resized as needed to accommodate the growing string representation, and proper character encoding is handled for multi-byte characters.

## Parameters / Member Variables
- : INFIX structure containing the output buffer, current position, operand strings, and current query item pointer
- : Priority level of the parent operator, used for parentheses decision-making
- : Boolean flag indicating whether this is the right operand of a phrase operator (affects precedence rules)

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth
  - RESIZEBUF
  - t_iseq
  - COPYCHAR
  - pg_mblen
  - pg_database_encoding_max_length
  - QO_PRIORITY
  - sprintf
  - strchr
  - pfree
- Called from (representative examples):
  - infix (recursive calls)
  - tsqueryout
  - tsquerytree

## Notes and Other Information
- This is a static function, only accessible within the tsquery.c module
- Includes stack depth checking to prevent stack overflow on deeply nested queries
- Properly handles character escaping for single quotes and backslashes in operand values
- Implements correct operator precedence: NOT (highest) > PHRASE > AND > OR (lowest)
- Special handling for phrase operators since they are not associative and order matters
- Uses dynamic buffer allocation with RESIZEBUF macro for efficient memory management
- Supports multi-byte character encodings through pg_mblen and related functions
- The function modifies the INFIX structure's current pointer as it builds the output string