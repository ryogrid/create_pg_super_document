# IsTidEqualClause

## Location
[src/backend/optimizer/path/tidpath.c:130-149](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/tidpath.c#L130-L149)

## Overview
IsTidEqualClause is a static function that checks whether a RestrictInfo represents a CTID equality clause suitable for TID-based access paths.

## Definition

```c
static bool
IsTidEqualClause(RestrictInfo *rinfo, RelOptInfo *rel)
```
## Detailed Description
This function determines if a RestrictInfo represents an equality clause of the form "CTID = pseudoconstant" or "pseudoconstant = CTID". It builds upon IsBinaryTidClause to first verify that the clause is a valid binary TID clause, then specifically checks that the operator is the TID equality operator. This function is essential for identifying clauses that can be used for direct TID-based tuple access in query execution.

## Parameters / Member Variables
- `*rinfo`: A RestrictInfo structure containing the clause to be examined
- `*rel`: A RelOptInfo structure representing the relation being analyzed
## Dependencies
- Functions called/Symbols referenced:
  - [IsBinaryTidClause](IsBinaryTidClause.md) (validates binary TID clause structure)
  - [OpExpr](../O/OpExpr.md) (cast to access operator information)
  - TIDEqualOperator (constant for TID equality operator)
- Called from (representative examples):
  - [RestrictInfoIsTidQual](../R/RestrictInfoIsTidQual.md)
  - [BuildParameterizedTidPaths](../B/BuildParameterizedTidPaths.md)

## Notes and Other Information
The function uses a layered approach: first calling IsBinaryTidClause to ensure the basic structure is correct (CTID variable with pseudoconstant), then verifying that the specific operator is equality. This design promotes code reuse and maintains clear separation of concerns between general binary TID clause validation and specific operator checking.

## Simplified Source

```c
static bool
IsTidEqualClause(RestrictInfo *rinfo, RelOptInfo *rel)
{
    // First check if it's a valid binary TID clause structure
    if (!IsBinaryTidClause(rinfo, rel))
        return false;

    // Then verify it's specifically an equality operator
    if (((OpExpr *) rinfo->clause)->opno == TIDEqualOperator)
        return true;

    return false;
}
```