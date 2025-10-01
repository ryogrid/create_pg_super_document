# BuildIndexValueDescription

## Location
[src/backend/access/index/genam.c:176-292](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/index/genam.c#L176-L292)

## Overview
Constructs a human-readable string representation of index entry contents in the form "(key_name, ...)=(key_value, ...)" for use in error messages.

## Definition

```c
char *
BuildIndexValueDescription(Relation indexRelation,
						   const Datum *values, const bool *isnull)
```
## Detailed Description
BuildIndexValueDescription creates a formatted string describing the contents of an index entry, primarily used for generating informative error messages in unique constraint and exclusion constraint violations. The function takes raw input values (as they would be passed to FormIndexDatum) and produces a readable representation that shows both column names and their corresponding values.

The function implements comprehensive security checks to prevent data leakage by verifying that the user has appropriate SELECT permissions on all key columns of the index. If Row Level Security (RLS) is enabled or if the user lacks sufficient permissions on any column, the function returns NULL rather than exposing potentially sensitive data. For expression-based indexes, it also returns NULL to avoid the complexity of determining which underlying columns are involved.

## Parameters / Member Variables
- : The index relation whose entry is being described
- : Array of Datum values representing the raw input to the index access method
- : Array of boolean flags indicating which values are NULL

## Dependencies
- Functions called/Symbols referenced:
  - IndexRelationGetNumberOfKeyAttributes (get key column count)
  - [check_enable_rls](../c/check_enable_rls.md) (Row Level Security check)
  - [pg_class_aclcheck](../p/pg_class_aclcheck.md) (table-level permission check)
  - [pg_attribute_aclcheck](../p/pg_attribute_aclcheck.md) (column-level permission check)
  - [pg_get_indexdef_columns](../p/pg_get_indexdef_columns.md) (get column names for display)
  - [getTypeOutputInfo](../g/getTypeOutputInfo.md) (get output function for data type)
  - [OidOutputFunctionCall](../O/OidOutputFunctionCall.md) (convert value to string representation)
- Called from (representative examples):
  - [_bt_check_unique](../b/_bt_check_unique.md) (B-tree unique constraint checking)
  - [check_exclusion_or_unique_constraint](../c/check_exclusion_or_unique_constraint.md) (constraint violation handling)
  - [comparetup_index_btree_tiebreak](../c/comparetup_index_btree_tiebreak.md) (tuple sorting operations)

## Notes and Other Information
- Returns NULL if the user lacks SELECT permissions on any key columns to prevent data leakage
- Uses the index opclass input type rather than the stored index type for value formatting
- Only processes key columns of the index, not included columns
- Handles expression-based indexes by returning NULL rather than trying to expose underlying column details
- The function respects Row Level Security policies and will not expose data when RLS is enabled
- Values are formatted using the appropriate output functions for their data types
- NULL values are displayed as the literal string "null" in the output

## Simplified Source

```c
char *
BuildIndexValueDescription(Relation indexRelation,
                          const Datum *values, const bool *isnull)
{
    StringInfoData buf;
    Form_pg_index idxrec;
    int indnkeyatts;
    Oid indexrelid = RelationGetRelid(indexRelation);
    Oid indrelid;

    indnkeyatts = IndexRelationGetNumberOfKeyAttributes(indexRelation);

    // Check permissions - return NULL if user lacks access
    idxrec = indexRelation->rd_index;
    indrelid = idxrec->indrelid;

    // Return NULL if RLS is enabled to avoid data leakage
    if (check_enable_rls(indrelid, InvalidOid, true) == RLS_ENABLED)
        return NULL;

    // Check table-level SELECT permission
    AclResult aclresult = pg_class_aclcheck(indrelid, GetUserId(), ACL_SELECT);
    if (aclresult != ACLCHECK_OK) {
        // No table-level access, check each column individually
        for (int keyno = 0; keyno < indnkeyatts; keyno++) {
            AttrNumber attnum = idxrec->indkey.values[keyno];

            // Return NULL for expression indexes or columns without permission
            if (attnum == InvalidAttrNumber ||
                pg_attribute_aclcheck(indrelid, attnum, GetUserId(),
                                    ACL_SELECT) != ACLCHECK_OK) {
                return NULL;
            }
        }
    }

    // Build the description string
    initStringInfo(&buf);
    appendStringInfo(&buf, "(%s)=(",
                    pg_get_indexdef_columns(indexrelid, true));

    for (int i = 0; i < indnkeyatts; i++) {
        char *val;

        if (isnull[i]) {
            val = "null";
        } else {
            // Get output function for the opclass input type
            Oid foutoid;
            bool typisvarlena;
            getTypeOutputInfo(indexRelation->rd_opcintype[i],
                            &foutoid, &typisvarlena);
            val = OidOutputFunctionCall(foutoid, values[i]);
        }

        if (i > 0)
            appendStringInfoString(&buf, ", ");
        appendStringInfoString(&buf, val);
    }

    appendStringInfoChar(&buf, ')');
    return buf.data;
}
```