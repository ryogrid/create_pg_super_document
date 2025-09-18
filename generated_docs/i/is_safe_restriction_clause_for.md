# is_safe_restriction_clause_for

## Location
src/backend/optimizer/util/orclauses.c: 126 - 155

## Overview
Determines whether a primitive (non-OR) RestrictInfo clause is safe to move to a specific relation as a restriction clause.

## Definition
```c
static bool is_safe_restriction_clause_for(RestrictInfo *rinfo, RelOptInfo *rel)
```

## Detailed Description
This static function performs safety checks to determine if a restriction clause can be safely applied to a specific relation during query optimization. The function ensures that moving the clause will not change query semantics or introduce performance issues.

The safety criteria include:
1. The clause must not be a pseudoconstant (constant expressions that don't actually restrict rows)
2. The clause must reference exactly the same set of relations as the target relation (no more, no less)  
3. The clause must not contain volatile functions that could produce different results on repeated evaluation

These checks ensure that extracting and duplicating the clause for early evaluation during relation scans will not affect correctness or introduce unwanted side effects.

## Parameters / Member Variables
- `rinfo`: RestrictInfo node containing the restriction clause to be evaluated for safety
- `rel`: RelOptInfo structure representing the target relation where the clause might be moved

## Dependencies
- Functions called/Symbols referenced:
  - [bms_equal](../b/bms_equal.md)
  - [contain_volatile_functions](../c/contain_volatile_functions.md)  
- Called from (representative examples):
  - [extract_or_clause](../e/extract_or_clause.md) (multiple times)

## Notes and Other Information
- This is a static function only used within the orclauses.c module
- The function is conservative - it only allows clause movement when it's completely safe
- Pseudoconstant clauses are quickly rejected since they don't provide meaningful restrictions
- The relids comparison ensures the clause doesn't reference relations outside the target relation's scope
- Volatile function detection prevents issues with functions that might return different values on repeated calls
- Used as a building block for more complex OR clause extraction logic