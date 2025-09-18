# TidRangeQualFromRestrictInfoList

## Location
[src/backend/optimizer/path/tidpath.c:387-414](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/tidpath.c#L387-L414)

## Overview
A static function that extracts CTID range conditions from a list of RestrictInfos, returning range qualifications suitable for TID range scan optimization if supported by the table's access method.

## Definition
```c
static List *TidRangeQualFromRestrictInfoList(List *rlist, RelOptInfo *rel)
```

## Detailed Description
TidRangeQualFromRestrictInfoList processes restriction clauses to identify those suitable for TID range scanning, which allows efficient access to ranges of tuples based on their physical storage positions. Unlike point TID lookups, range scans can process inequality conditions on CTID values.

The function implements a straightforward filtering process:
1. **Access Method Validation**: First checks if the relation's access method supports TID range operations using the AMFLAG_HAS_TID_RANGE flag
2. **Range Clause Detection**: Iterates through all restriction clauses, testing each with IsTidRangeClause to identify valid range conditions
3. **AND Semantics**: Accumulates all valid range clauses with implicit AND semantics, allowing multiple range constraints to be combined

This function is simpler than its counterpart TidQualFromRestrictInfoList because range scans don't require the complex OR/AND handling needed for point lookups, and CurrentOfExpr clauses are not applicable to range operations.

## Parameters / Member Variables
- `rlist`: List of RestrictInfo structures representing the restriction clauses to analyze for range conditions
- `rel`: RelOptInfo pointer representing the target relation, including access method capabilities

## Dependencies
- Functions called/Symbols referenced:
  - AMFLAG_HAS_TID_RANGE (access method flag)
  - [IsTidRangeClause](../I/IsTidRangeClause.md)
  - lappend
- Called from (representative examples):
  - [create_tidscan_paths](../c/create_tidscan_paths.md)

## Notes and Other Information
- Static function accessible only within tidpath.c
- Returns NIL if the access method doesn't support TID range scans
- Much simpler than point TID qualification extraction due to different semantic requirements
- Returns list with implicit AND semantics across all range conditions
- Part of PostgreSQL's TID range scan optimization infrastructure
- Access method capability check prevents unnecessary processing for unsupported storage engines
- Range scans are useful for inequality conditions on CTID values (e.g., ctid > '(1,1)' AND ctid < '(10,100)')