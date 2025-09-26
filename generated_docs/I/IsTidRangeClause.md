# IsTidRangeClause

## Location
[src/backend/optimizer/path/tidpath.c:150-171](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/tidpath.c#L150-L171)

## Overview
IsTidRangeClause is a static function that identifies whether a RestrictInfo represents a CTID range comparison clause suitable for TID range scanning.

## Definition

```c
static bool
IsTidRangeClause(RestrictInfo *rinfo, RelOptInfo *rel)
```
## Detailed Description
This function determines if a RestrictInfo represents a range comparison clause of the form "CTID OP pseudoconstant" or "pseudoconstant OP CTID", where OP is a range operator (<, <=, >, or >=). It first validates the clause structure using IsBinaryTidClause, then specifically checks that the operator is one of the supported TID range operators. This function is crucial for identifying clauses that can be used for TID range scanning, which allows efficient scanning of tuple identifier ranges.

## Parameters / Member Variables
- : A RestrictInfo structure containing the clause to be examined
- : A RelOptInfo structure representing the relation being analyzed

## Dependencies
- Functions called/Symbols referenced:
  - [IsBinaryTidClause](IsBinaryTidClause.md) (validates binary TID clause structure)
  - [OpExpr](../O/OpExpr.md) (cast to access operator information)
  - TIDLessOperator (constant for TID < operator)
  - TIDLessEqOperator (constant for TID <= operator)
  - TIDGreaterOperator (constant for TID > operator)
  - TIDGreaterEqOperator (constant for TID >= operator)
- Called from (representative examples):
  - [TidRangeQualFromRestrictInfoList](../T/TidRangeQualFromRestrictInfoList.md)

## Notes and Other Information
The function supports four range operators for TID comparisons: less than, less than or equal, greater than, and greater than or equal. Like IsTidEqualClause, it uses a layered approach by first calling IsBinaryTidClause to ensure proper structure, then checking for specific range operators. This enables the query optimizer to consider TID range scans when appropriate range conditions are present.