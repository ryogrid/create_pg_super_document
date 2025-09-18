# addRangeClause

## Location
src/backend/optimizer/path/clausesel.c: 427 - 522

## Overview
Manages and optimizes range query clauses by pairing inequality operators on the same variable and maintaining the most restrictive bounds for accurate selectivity estimation.

## Definition
```c
static void addRangeClause(RangeQueryClause **rqlist, Node *clause,
                          bool varonleft, bool isLTsel, Selectivity s2)
```

## Detailed Description
This function is a key component of PostgreSQL's range query optimization system, responsible for intelligently organizing and consolidating inequality clauses that operate on the same variable. The function implements several critical optimizations:

1. **Clause Pairing**: Attempts to match new range clauses with existing ones that reference the same variable, enabling the optimizer to recognize patterns like "x > 10 AND x < 50" as a single range constraint.

2. **Bound Classification**: Determines whether each clause represents a lower bound or upper bound based on:
   - Variable position (left vs right side of operator)
   - Operator type (less-than vs greater-than family)
   - The logical combination of these factors

3. **Redundancy Elimination**: When multiple clauses of the same bound type are found (e.g., "x > 10 AND x > 5"), automatically keeps only the more restrictive constraint by comparing selectivities.

4. **Data Structure Management**: Maintains a linked list of RangeQueryClause structures, creating new entries for variables not yet seen and updating existing entries for known variables.

The function is essential for converting multiple independent inequality estimates into accurate range selectivity calculations, significantly improving cost estimation for range queries.

## Parameters / Member Variables
- `rqlist`: Pointer to the head of the range query clause linked list (modified in-place)
- `clause`: The inequality clause node being processed and added to the range system
- `varonleft`: Boolean indicating whether the variable appears on the left side of the operator
- `isLTsel`: Boolean indicating whether this is a less-than type selectivity (< or <=)
- `s2`: Pre-calculated selectivity value for this specific clause

## Dependencies
- Functions called/Symbols referenced:
  - [get_leftop](../g/get_leftop.md)
  - [get_rightop](../g/get_rightop.md)
  - [equal](../e/equal.md)
  - [palloc](../p/palloc.md)
  - [RangeQueryClause](../R/RangeQueryClause.md)
- Called from (representative examples):
  - [clauselist_selectivity_ext](../c/clauselist_selectivity_ext.md)

## Notes and Other Information
This function implements sophisticated logic for bound determination:
- For "x < value" (varonleft=true, isLTsel=true): Creates a high bound
- For "value < x" (varonleft=false, isLTsel=true): Creates a low bound  
- For "x > value" (varonleft=true, isLTsel=false): Creates a low bound
- For "value > x" (varonleft=false, isLTsel=false): Creates a high bound

Key implementation features:
- Uses full equal() comparison to handle complex variable expressions and function calls
- Implements "keep most restrictive" policy for redundant constraints (lower selectivity = more restrictive)
- Maintains linked list structure for efficient traversal during range processing
- Memory management via palloc for new RangeQueryClause structures
- The function is static, indicating internal use within the clausesel.c module
- Critical for the range query optimization that converts "hisel * losel" to "hisel + losel - 1" calculations
- Handles edge cases where the same variable appears in multiple inequality expressions with different comparison operators