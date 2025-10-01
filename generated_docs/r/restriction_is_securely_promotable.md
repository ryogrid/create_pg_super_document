# restriction_is_securely_promotable

## Location
[src/backend/optimizer/util/restrictinfo.c:431-452](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/restrictinfo.c#L431-L452)

## Overview
Determines whether a restriction clause can be safely evaluated "early" before other restriction clauses attached to a specific relation, based on security considerations.

## Definition
```c
bool restriction_is_securely_promotable(RestrictInfo *restrictinfo, RelOptInfo *rel)
```

## Detailed Description
This function implements PostgreSQL's row-level security (RLS) mechanism by controlling the evaluation order of restriction clauses. It ensures that security-sensitive clauses are evaluated in the correct order to prevent information leakage. The function returns true if a given restriction clause can be safely promoted (evaluated early) without violating security policies.

The security model works by assigning security levels to different types of clauses and ensuring that higher-security clauses (like RLS policies) are evaluated before lower-security clauses (like user-provided WHERE conditions) that might leak information about filtered-out rows.

## Parameters / Member Variables
- `restrictinfo`: Pointer to RestrictInfo structure containing the restriction clause to be evaluated, including its security level and leakproof status
- `rel`: Pointer to RelOptInfo structure representing the relation, which contains the minimum security level required for base restriction clauses

## Dependencies
- Functions called/Symbols referenced:
  - [RestrictInfo](../R/RestrictInfo.md) struct (security_level, leakproof fields)
  - [RelOptInfo](../R/RelOptInfo.md) struct (baserestrict_min_security field)
- Called from (representative examples):
  - [match_clause_to_index](../m/match_clause_to_index.md) (src/backend/optimizer/path/indxpath.c:2104)
  - [RestrictInfoIsTidQual](../R/RestrictInfoIsTidQual.md) (src/backend/optimizer/path/tidpath.c:247)
  - [BuildParameterizedTidPaths](../B/BuildParameterizedTidPaths.md) (src/backend/optimizer/path/tidpath.c:440)
  - make_simple_restrictinfo (src/include/optimizer/restrictinfo.h:37)

## Notes and Other Information
- The function implements a simple but critical security check: a clause can be promoted if either its security level is at or below the relation's minimum required security level, or if the clause is marked as "leakproof"
- Leakproof functions are those certified to not reveal information about their inputs when they return false or raise an error
- This mechanism is essential for row-level security (RLS) implementations where policy clauses must be evaluated before user clauses to prevent information disclosure attacks
- The security level comparison ensures that security policies are applied before potentially information-leaking user-defined conditions

## Simplified Source

```c
bool
restriction_is_securely_promotable(RestrictInfo *restrictinfo, RelOptInfo *rel)
{
    // Allow promotion if either:
    // 1. Security level is low enough (at or below minimum required), OR
    // 2. Function is marked as leakproof (certified safe)
    if (restrictinfo->security_level <= rel->baserestrict_min_security ||
        restrictinfo->leakproof)
        return true;
    else
        return false;
}
```