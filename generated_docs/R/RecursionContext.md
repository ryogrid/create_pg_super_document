# RecursionContext

## Location
src/backend/parser/parse_cte.c: 39 - 62

## Overview
RecursionContext is an enumeration that defines the different contexts in which recursive self-references are either allowed or disallowed within Common Table Expressions (CTEs) in PostgreSQL.

## Definition
```c
typedef enum
{
    RECURSION_OK,
    RECURSION_NONRECURSIVETERM, /* inside the left-hand term */
    RECURSION_SUBLINK,          /* inside a sublink */
    RECURSION_OUTERJOIN,        /* inside nullable side of an outer join */
    RECURSION_INTERSECT,        /* underneath INTERSECT (ALL) */
    RECURSION_EXCEPT,           /* underneath EXCEPT (ALL) */
} RecursionContext;
```

## Detailed Description
RecursionContext is used by PostgreSQL's parser to enforce the SQL standard's restrictions on recursive Common Table Expressions (WITH RECURSIVE). The enumeration tracks the current parsing context to determine whether a self-reference to a recursive CTE is valid or should trigger an error.

When parsing recursive CTEs, PostgreSQL needs to ensure that recursive references follow specific rules defined by the SQL standard. This enumeration helps the parser maintain state about which syntactic contexts are currently being processed, allowing it to reject invalid recursive references with appropriate error messages.

The enumeration works in conjunction with a corresponding error message array (`recursion_errormsgs`) that provides descriptive error messages for each forbidden context.

## Parameters / Member Variables
- `RECURSION_OK`: Recursive self-references are allowed in this context
- `RECURSION_NONRECURSIVETERM`: Self-references are forbidden within the non-recursive (left-hand) term of a UNION
- `RECURSION_SUBLINK`: Self-references are forbidden within subqueries/sublinks
- `RECURSION_OUTERJOIN`: Self-references are forbidden within the nullable side of an outer join
- `RECURSION_INTERSECT`: Self-references are forbidden underneath INTERSECT operations
- `RECURSION_EXCEPT`: Self-references are forbidden underneath EXCEPT operations

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is an enumeration type)
- Called from (representative examples):
  - [CteState](../C/CteState.md) (as a member variable at src/backend/parser/parse_cte.c:82)
  - [checkWellFormedRecursionWalker](../c/checkWellFormedRecursionWalker.md) (context tracking at src/backend/parser/parse_cte.c:1029)
  - [checkWellFormedSelectStmt](../c/checkWellFormedSelectStmt.md) (context management at src/backend/parser/parse_cte.c:1209)

## Notes and Other Information
- This enumeration is part of PostgreSQL's WITH RECURSIVE implementation and enforces SQL standard compliance
- Each enumeration value (except RECURSION_OK) has a corresponding error message in the `recursion_errormsgs` array
- The context is managed through a save-and-restore pattern in the tree walker functions to maintain proper nesting state
- Used specifically during the validation phase of recursive CTE processing to ensure well-formed recursive queries
- The enumeration helps provide clear, context-specific error messages when users write invalid recursive queries