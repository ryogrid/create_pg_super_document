# checkRuleResultList

## Location
[src/backend/rewrite/rewriteDefine.c:506-630](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteDefine.c#L506-L630)

## Overview
checkRuleResultList validates that a target list (either SELECT or RETURNING) produces output that is compatible with a relation's tuple descriptor, ensuring type and structure consistency.

## Definition

```c
static void
checkRuleResultList(List *targetList, TupleDesc resultDesc, bool isSelect,
					bool requireColumnNameMatch)
```
## Detailed Description
checkRuleResultList performs comprehensive validation of target lists against relation schemas to ensure compatibility when creating rules. It validates that the number of entries matches the relation's attribute count, verifies type compatibility between target list expressions and corresponding relation columns, checks column name matching when required (for SELECT rules), and handles type modifier (typmod) validation with appropriate flexibility for unspecified cases. The function also prevents operations on relations with dropped columns, as supporting them would require significant infrastructure changes. It provides detailed error messages distinguishing between SELECT target lists and RETURNING lists for better user feedback.

## Parameters / Member Variables
- `*targetList`: List of TargetEntry nodes representing the output columns to validate
- `resultDesc`: TupleDesc describing the expected schema of the target relation
- `isSelect`: Boolean flag indicating whether this is a SELECT target list (vs RETURNING list) for error message context
- `requireColumnNameMatch`: Boolean requiring exact column name matching (only valid when isSelect is true)
## Dependencies
- Functions called/Symbols referenced:
  - [TargetEntry](../T/TargetEntry.md) (struct access)
  - TupleDescAttr
  - NameStr
  - [exprType](../e/exprType.md)
  - [exprTypmod](../e/exprTypmod.md)
  - [format_type_be](../f/format_type_be.md)
  - [format_type_with_typemod](../f/format_type_with_typemod.md)
- Called from (representative examples):
  - [DefineQueryRewrite](../D/DefineQueryRewrite.md) (twice - for SELECT rules and RETURNING validation)

## Notes and Other Information
- This is a static function internal to rewriteDefine.c used specifically for rule validation
- Ignores resjunk (junk result) entries in the target list as they don't correspond to output columns
- Enforces strict type matching but allows typmod differences when one is unspecified (-1)
- Prevents creation of rules on relations with dropped columns due to implementation complexity
- Provides context-sensitive error messages that distinguish between SELECT rules and RETURNING lists
- Critical for maintaining data integrity when creating view rules and RETURNING clause validation
- The requireColumnNameMatch parameter is only used for SELECT rules on views where exact column name correspondence is required

## Simplified Source

```c
static void checkRuleResultList(List *targetList, TupleDesc resultDesc,
                               bool isSelect, bool requireColumnNameMatch) {
    ListCell *tllist;
    int i = 0;

    // Validate each non-junk target entry
    foreach(tllist, targetList) {
        TargetEntry *tle = (TargetEntry *) lfirst(tllist);

        if (tle->resjunk)
            continue;

        i++;

        // Check if we have too many entries
        if (i > resultDesc->natts)
            ereport(ERROR,
                   (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                    isSelect ?
                    errmsg("SELECT rule's target list has too many entries") :
                    errmsg("RETURNING list has too many entries")));

        Form_pg_attribute attr = TupleDescAttr(resultDesc, i - 1);
        char *attname = NameStr(attr->attname);

        // Reject dropped columns
        if (attr->attisdropped)
            ereport(ERROR,
                   (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    isSelect ?
                    errmsg("cannot convert relation containing dropped columns to view") :
                    errmsg("cannot create a RETURNING list for a relation containing dropped columns")));

        // Check column name match if required
        if (requireColumnNameMatch && strcmp(tle->resname, attname) != 0)
            ereport(ERROR,
                   (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                    errmsg("SELECT rule's target entry %d has different column name from column \"%s\"",
                           i, attname)));

        // Check type compatibility
        Oid tletypid = exprType((Node *) tle->expr);
        if (attr->atttypid != tletypid)
            ereport(ERROR,
                   (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                    errmsg("target entry %d has different type from column \"%s\"", i, attname)));

        // Check type modifier compatibility (allow if either is unspecified)
        int32 tletypmod = exprTypmod((Node *) tle->expr);
        if (attr->atttypmod != tletypmod &&
            attr->atttypmod != -1 && tletypmod != -1)
            ereport(ERROR,
                   (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                    errmsg("target entry %d has different size from column \"%s\"", i, attname)));
    }

    // Check if we have too few entries
    if (i != resultDesc->natts)
        ereport(ERROR,
               (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                isSelect ?
                errmsg("SELECT rule's target list has too few entries") :
                errmsg("RETURNING list has too few entries")));
}
```