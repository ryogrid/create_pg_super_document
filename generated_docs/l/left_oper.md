# left_oper

## Location
[src/backend/parser/parse_oper.c:518-601](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_oper.c#L518-L601)

## Overview
The  function searches for a unary left operator (prefix operator) given an operator name and argument type, returning the operator structure for PostgreSQL's parser.

## Definition

```c
Operator
left_oper(ParseState *pstate, List *op, Oid arg, bool noError, int location)
```
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
  - [make_oper_cache_key](../m/make_oper_cache_key.md)
  - [find_oper_cache_entry](../f/find_oper_cache_entry.md)
  - [OpernameGetOprid](../O/OpernameGetOprid.md)
  - [OpernameGetCandidates](../O/OpernameGetCandidates.md)
  - [oper_select_candidate](../o/oper_select_candidate.md)
  - [make_oper_cache_entry](../m/make_oper_cache_entry.md)
  - [op_error](../o/op_error.md)
- Called from (representative examples):
  - [make_op](../m/make_op.md)
  - [generate_operator_name](../g/generate_operator_name.md)

## Notes and Other Information
- Returns a syscache entry that must be released with ReleaseSysCache() when done
- Only guarantees coercion-compatibility, not exact or binary compatibility
- Uses a lookaside cache for performance optimization
- Implements a fallback mechanism when exact matches are not found
- The function modifies the candidate list structure during processing by moving argument data for compatibility with oper_select_candidate