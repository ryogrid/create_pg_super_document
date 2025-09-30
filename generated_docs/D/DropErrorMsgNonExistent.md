# DropErrorMsgNonExistent

## Location
[src/backend/commands/tablecmds.c:1393-1440](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L1393-L1440)

## Overview
DropErrorMsgNonExistent generates appropriate error or warning messages when a DROP command is issued on a non-existent relation.

## Definition

```c
struct dropmsgstrings *rentry;
```
## Detailed Description
DropErrorMsgNonExistent is a static helper function responsible for providing user-friendly error messages when attempting to drop relations that do not exist. The function first checks if the specified schema exists, and if not, reports an appropriate schema-related error or notice. Then it looks up the relation kind in the dropmsgstringarray to find the corresponding error messages. Depending on the missing_ok flag, it either throws an error (when missing_ok is false) or issues a notice and continues (when missing_ok is true). This supports the IF EXISTS clause behavior in DROP statements.

## Parameters / Member Variables
- : RangeVar structure containing the relation name and schema information
- : Character indicating the expected relation type (table, view, index, etc.)
- : Boolean flag indicating whether to issue a notice instead of an error for non-existent relations

## Dependencies
- Functions called/Symbols referenced:
  - [LookupNamespaceNoError](../L/LookupNamespaceNoError.md)
  - dropmsgstringarray
  - ereport
  - Assert
- Called from (representative examples):
  - [RemoveRelations](../R/RemoveRelations.md)

## Notes and Other Information
DropErrorMsgNonExistent is designed to provide consistent and informative error messages across different DROP operations. The function uses a static array (dropmsgstringarray) that maps relation kinds to their corresponding error codes and message templates. The two-phase check (schema existence, then relation existence) ensures that users get the most specific error message possible. The missing_ok parameter enables the IF EXISTS functionality common in DDL operations, allowing scripts to be more robust by not failing on missing objects.

## Simplified Source

```c
static void DropErrorMsgNonExistent(RangeVar *rel, char rightkind, bool missing_ok) {
    const struct dropmsgstrings *rentry;

    // First check if schema exists (if schema is specified)
    if (rel->schemaname != NULL && !OidIsValid(LookupNamespaceNoError(rel->schemaname))) {
        if (!missing_ok) {
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_SCHEMA),
                           errmsg("schema \"%s\" does not exist", rel->schemaname)));
        } else {
            ereport(NOTICE, (errmsg("schema \"%s\" does not exist, skipping", rel->schemaname)));
        }
        return;
    }

    // Find the appropriate error message for this relation type
    for (rentry = dropmsgstringarray; rentry->kind != '\0'; rentry++) {
        if (rentry->kind == rightkind) {
            if (!missing_ok) {
                // Issue error for missing relation
                ereport(ERROR, (errcode(rentry->nonexistent_code),
                               errmsg(rentry->nonexistent_msg, rel->relname)));
            } else {
                // Issue notice and continue (IF EXISTS behavior)
                ereport(NOTICE, (errmsg(rentry->skipping_msg, rel->relname)));
                break;
            }
        }
    }

    Assert(rentry->kind != '\0'); // Should find a matching entry
}
```