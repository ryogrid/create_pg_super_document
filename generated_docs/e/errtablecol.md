# errtablecol

## Location
[src/backend/utils/cache/relcache.c:5974-5997](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L5974-L5997)

## Overview
Stores schema name, table name, and column name of a table column within the current error context, accepting column specification by attribute number.

## Definition
```c
int errtablecol(Relation rel, int attnum)
```

## Detailed Description
This function enhances error reporting by capturing column-specific context information and storing it in the current error data structure. It takes a relation and an attribute number, resolves the column name, and then delegates to errtablecolname() to store the complete table and column context in the error reporting framework.

The function provides a convenient interface for callers who have an attribute number rather than a column name, handling the resolution of attribute number to column name internally. It optimizes for user attributes by using the relation's tuple descriptor when possible, falling back to catalog lookups for system attributes or when the attribute number is out of range.

## Parameters / Member Variables
- `rel`: The relation (table) containing the column
- `attnum`: The attribute number of the column (1-based for user attributes, negative for system attributes)

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetDescr
  - TupleDescAttr
  - NameStr
  - [get_attname](../g/get_attname.md)
  - RelationGetRelid
  - [errtablecolname](errtablecolname.md)
- Called from (representative examples):
  - [ATRewriteTable](../A/ATRewriteTable.md)
  - [validateDomainNotNullConstraint](../v/validateDomainNotNullConstraint.md)
  - [validateDomainCheckConstraint](../v/validateDomainCheckConstraint.md)
  - [ExecConstraints](../E/ExecConstraints.md)

## Notes and Other Information
- Uses the relation's tuple descriptor for efficiency when dealing with user attributes (attnum > 0)
- Falls back to catalog lookup via get_attname() for system attributes or out-of-range attribute numbers
- Designed to be more convenient than errtablecolname() when callers have attribute numbers rather than names
- Part of PostgreSQL's structured error reporting system for enhanced debugging
- The return value (0) does not matter and is ignored by callers

## Simplified Source

```c
int errtablecol(Relation rel, int attnum) {
    TupleDesc reldesc = RelationGetDescr(rel);
    const char *colname;

    // Get column name: use relation descriptor for user attributes, catalog lookup for others
    if (attnum > 0 && attnum <= reldesc->natts)
        colname = NameStr(TupleDescAttr(reldesc, attnum - 1)->attname);
    else
        colname = get_attname(RelationGetRelid(rel), attnum, false);

    // Store table and column info in error context
    return errtablecolname(rel, colname);
}
```