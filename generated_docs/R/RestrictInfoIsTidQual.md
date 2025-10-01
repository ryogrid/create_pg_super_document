# RestrictInfoIsTidQual

## Location
[src/backend/optimizer/path/tidpath.c:234-279](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/tidpath.c#L234-L279)

## Overview
A static function that determines whether a RestrictInfo clause can be used as a CTID (Current Tuple ID) qualification for a specified relation, considering security and validity constraints.

## Definition
```c
static bool RestrictInfoIsTidQual(PlannerInfo *root, RestrictInfo *rinfo, RelOptInfo *rel)
```

## Detailed Description
RestrictInfoIsTidQual evaluates whether a given restriction clause is suitable for TID-based access patterns. It performs comprehensive validation including security checks and clause type verification. The function is part of PostgreSQL's TID scan optimization infrastructure, which allows direct tuple access using physical tuple identifiers.

The function implements a multi-layered validation process:
1. Rejects pseudoconstant clauses (cannot contain variables)
2. Enforces security restrictions using restriction_is_securely_promotable
3. Validates against three specific TID qualification patterns: equality clauses, equality-any clauses, and current-of clauses

This function handles only base cases; complex AND/OR combinations are processed by higher-level functions.

## Parameters / Member Variables
- `root`: PlannerInfo pointer containing global planning state and context
- `rinfo`: RestrictInfo pointer representing the clause to evaluate  
- `rel`: RelOptInfo pointer representing the target relation

## Dependencies
- Functions called/Symbols referenced:
  - [restriction_is_securely_promotable](../r/restriction_is_securely_promotable.md)
  - [IsTidEqualClause](../I/IsTidEqualClause.md)
  - [IsTidEqualAnyClause](../I/IsTidEqualAnyClause.md)  
  - [IsCurrentOfClause](../I/IsCurrentOfClause.md)
- Called from (representative examples):
  - [TidQualFromRestrictInfoList](../T/TidQualFromRestrictInfoList.md)

## Notes and Other Information
- Static function accessible only within tidpath.c
- Security-aware: rejects clauses that cannot be safely promoted due to security levels
- Part of PostgreSQL's cost-based optimizer for TID scan path generation
- Does not handle complex boolean expressions (AND/OR) - these are processed by calling functions
- Essential for enabling direct tuple access optimizations in query execution

## Simplified Source

```c
static bool
RestrictInfoIsTidQual(PlannerInfo *root, RestrictInfo *rinfo, RelOptInfo *rel)
{
    // Skip pseudoconstant clauses (cannot contain variables)
    if (rinfo->pseudoconstant)
        return false;

    // Check security restrictions - ensure safe evaluation order
    if (!restriction_is_securely_promotable(rinfo, rel))
        return false;

    // Check all supported TID qualification patterns
    if (IsTidEqualClause(rinfo, rel) ||           // CTID = constant
        IsTidEqualAnyClause(root, rinfo, rel) ||  // CTID = ANY(array)
        IsCurrentOfClause(rinfo, rel))            // CURRENT OF cursor
        return true;

    return false;
}
```