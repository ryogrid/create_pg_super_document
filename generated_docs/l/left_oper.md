# left_oper

## Location
src/backend/parser/parse_oper.c: 518 - 601

## Overview
The  function searches for a unary left operator (prefix operator) given an operator name and argument type, returning the operator structure for PostgreSQL's parser.

## Definition


## Detailed Description
This function is responsible for finding unary prefix operators in PostgreSQL's parser. It implements a two-phase search strategy: first attempting an exact match, then falling back to finding the most suitable candidate operator through type compatibility analysis. The function utilizes a lookaside cache to optimize repeated lookups and supports both error-raising and error-suppressing modes of operation.

The function is particularly important in the parsing phase where prefix operators like unary minus (-x) or logical NOT need to be resolved. It ensures that the returned operator is coercion-compatible with the input datatype, though it does not guarantee exact or binary compatibility.

## Parameters / Member Variables
- : ParseState pointer used for error reporting and context
- : List containing the operator name to search for
- : OID of the argument type for the unary operator
- : Boolean flag - if true, returns NULL on failure; if false, raises an error
- : Source location for error reporting (-1 if not available)

## Dependencies
- Functions called/Symbols referenced:
  - make_oper_cache_key
  - find_oper_cache_entry
  - OpernameGetOprid
  - OpernameGetCandidates
  - oper_select_candidate
  - make_oper_cache_entry
  - op_error
- Called from (representative examples):
  - make_op
  - generate_operator_name

## Notes and Other Information
- Returns a syscache entry that must be released with ReleaseSysCache() when done
- Only guarantees coercion-compatibility, not exact or binary compatibility
- Uses a lookaside cache for performance optimization
- Implements a fallback mechanism when exact matches are not found
- The function modifies the candidate list structure during processing by moving argument data for compatibility with oper_select_candidate