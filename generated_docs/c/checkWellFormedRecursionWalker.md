# checkWellFormedRecursionWalker

## Location
src/backend/parser/parse_cte.c: 1027 - 1206

## Overview
checkWellFormedRecursionWalker is a recursive tree walker function that traverses SQL parse trees to detect and validate self-references in recursive CTE queries, ensuring they appear only in valid contexts and with proper frequency.

## Definition
```c
static bool checkWellFormedRecursionWalker(Node *node, CteState *cstate)
```

## Detailed Description
This function implements a specialized tree walker that enforces PostgreSQL's rules for recursive CTE self-references. It performs context-aware validation by:

**Self-Reference Detection:**
- Identifies RangeVar nodes that reference the current recursive CTE being validated
- Checks inner WITH clause scope to ensure references aren't captured by nested CTEs
- Counts self-references to ensure exactly one appears in the recursive term

**Context-Sensitive Validation:**
- **RECURSION_OK**: Valid context where self-references are allowed
- **RECURSION_NONRECURSIVETERM**: Non-recursive term where self-references are forbidden
- **RECURSION_SUBLINK**: Subqueries where self-references are forbidden
- **RECURSION_OUTERJOIN**: Outer join contexts where self-references have restrictions

**Special Node Handling:**
- **SelectStmt**: Handles nested WITH clauses with proper visibility scoping (recursive vs non-recursive)
- **JoinExpr**: Applies context restrictions for different join types (outer joins change context to RECURSION_OUTERJOIN)
- **SubLink**: Changes context to RECURSION_SUBLINK for subquery validation
- **WithClause**: Prevents uncontrolled recursion into nested WITH clauses

**Visibility Management:**
- Maintains innerwiths stack to track CTE visibility at different nesting levels
- Implements different scoping rules for recursive vs non-recursive WITH clauses
- Ensures proper CTE name resolution according to SQL standard semantics

The walker integrates with the generic raw_expression_tree_walker for comprehensive tree traversal while providing specialized handling for recursion-sensitive constructs.

## Parameters / Member Variables
- `node`: The current parse tree node being examined
- `cstate`: CTE validation state containing current item context, recursion counters, inner WITH scope stack, and error reporting information

## Dependencies
- Functions called/Symbols referenced:
  - raw_expression_tree_walker (generic tree traversal)
  - [checkWellFormedSelectStmt](checkWellFormedSelectStmt.md) (SELECT statement validation)
  - ereport (error reporting)
  - [errcode](../e/errcode.md) (error code specification) 
  - [errmsg](../e/errmsg.md) (error message formatting)
  - [parser_errposition](../p/parser_errposition.md) (parse location for errors)
  - strcmp (string comparison)
  - [lcons](../l/lcons.md) (list construction)
  - lappend (list append)
  - list_delete_first (list manipulation)
  - list_head (list access)
  - IsA (type checking macro)
  - elog (internal error logging)
  - [RecursionContext](../R/RecursionContext.md) (recursion context enum)
  - [RangeVar](../R/RangeVar.md) (table reference structure)
  - SelectStmt (SELECT statement structure)
  - JoinExpr (join expression structure)
  - SubLink (sublink structure)
  - WithClause (WITH clause structure)
  - CommonTableExpr (CTE structure)
  - RECURSION_* constants (context enumeration values)
  - JOIN_* constants (join type enumeration values)

- Called from:
  - [checkWellFormedRecursion](checkWellFormedRecursion.md) (main validation controller)
  - [checkWellFormedSelectStmt](checkWellFormedSelectStmt.md) (SELECT statement processing)
  - Self-recursively for tree traversal

## Notes and Other Information
- The walker implements a context-sensitive state machine for recursion validation
- Different SQL constructs impose different restrictions on where recursive self-references can appear
- The function maintains proper scoping for nested WITH clauses to prevent incorrect reference capture
- Error messages provide specific parse locations and context-appropriate explanations
- The walker handles both recursive and non-recursive WITH clauses with different visibility semantics
- Outer join handling is critical because recursive references in outer join contexts can produce incorrect results
- The function prevents infinite loops by controlling WITH clause recursion and limiting raw_expression_tree_walker usage
- Self-reference counting ensures the recursive term has exactly one self-reference (more or fewer is invalid)