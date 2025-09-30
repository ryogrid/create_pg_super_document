# ExecSecLabelStmt

## Location
[src/backend/commands/seclabel.c:115-223](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/seclabel.c#L115-L223)

## Overview
Executes a SECURITY LABEL statement to apply a security label to a database object, handling provider validation, object type checking, and label application.

## Definition
```c
ObjectAddress ExecSecLabelStmt(SecLabelStmt *stmt)
```

## Detailed Description
This function is the main entry point for processing SECURITY LABEL SQL statements in PostgreSQL. It orchestrates the entire process of applying security labels to database objects, including:

1. **Provider Resolution**: Determines which security label provider to use. If no provider is specified in the statement, it checks if exactly one provider is loaded and uses it. If multiple providers are loaded, it requires explicit specification.

2. **Object Type Validation**: Uses SecLabelSupportsObjectType() to verify that security labels are supported for the target object type.

3. **Object Address Resolution**: Converts the parser representation of the target object into an ObjectAddress using get_object_address(), which also acquires necessary locks.

4. **Ownership Verification**: Ensures the current user has ownership privileges on the target object.

5. **Object-Specific Validation**: Performs additional integrity checks specific to certain object types (e.g., for columns, validates the relation kind).

6. **Provider Hook Execution**: Allows the security label provider to validate or veto the new label through its hook function.

7. **Label Application**: Applies the security label using SetSecurityLabel().

The function maintains proper resource management by closing relations opened during object address resolution while retaining locks until commit time.

## Parameters / Member Variables
- `stmt`: A pointer to a SecLabelStmt structure containing the parsed SECURITY LABEL statement, including the provider name, object type, target object, and label text

## Dependencies
- Functions called/Symbols referenced:
  - [SecLabelSupportsObjectType](../S/SecLabelSupportsObjectType.md)
  - [get_object_address](../g/get_object_address.md)
  - [check_object_ownership](../c/check_object_ownership.md)
  - [SetSecurityLabel](../S/SetSecurityLabel.md)
  - [relation_close](../r/relation_close.md)
  - [GetUserId](../G/GetUserId.md)
  - RelationGetRelationName
  - [errdetail_relkind_not_supported](../e/errdetail_relkind_not_supported.md)
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- Returns an ObjectAddress representing the object to which the security label was applied
- Acquires a ShareUpdateExclusiveLock on the target object to prevent concurrent modifications during the operation
- For OBJECT_COLUMN targets, performs additional validation to ensure the column belongs to a supported relation kind (tables, views, materialized views, composite types, foreign tables, or partitioned tables)
- The function supports both explicit provider specification and automatic provider selection when only one provider is loaded
- Security label providers can veto label application through their hook functions by throwing errors
- Maintains transactional semantics - locks are held until commit time to ensure consistency

## Simplified Source

```c
ObjectAddress ExecSecLabelStmt(SecLabelStmt *stmt) {
    LabelProvider *provider = NULL;
    ObjectAddress address;
    Relation relation;
    ListCell *lc;

    // Find the security label provider
    if (stmt->provider == NULL) {
        // No provider specified - use the only one if exactly one is loaded
        if (label_provider_list == NIL) {
            ereport(ERROR, "no security label providers have been loaded");
        }
        if (list_length(label_provider_list) != 1) {
            ereport(ERROR, "must specify provider when multiple security label providers have been loaded");
        }
        provider = (LabelProvider *) linitial(label_provider_list);
    } else {
        // Find the specified provider
        foreach(lc, label_provider_list) {
            LabelProvider *lp = lfirst(lc);
            if (strcmp(stmt->provider, lp->provider_name) == 0) {
                provider = lp;
                break;
            }
        }
        if (provider == NULL) {
            ereport(ERROR, "security label provider \"%s\" is not loaded", stmt->provider);
        }
    }

    // Check if object type supports security labels
    if (!SecLabelSupportsObjectType(stmt->objtype)) {
        ereport(ERROR, "security labels are not supported for this type of object");
    }

    // Resolve the target object and acquire lock
    address = get_object_address(stmt->objtype, stmt->object,
                               &relation, ShareUpdateExclusiveLock, false);

    // Check ownership
    check_object_ownership(GetUserId(), stmt->objtype, address, stmt->object, relation);

    // Additional validation for column objects
    switch (stmt->objtype) {
        case OBJECT_COLUMN:
            // Only allow labels on columns of supported relation kinds
            if (relation->rd_rel->relkind != RELKIND_RELATION &&
                relation->rd_rel->relkind != RELKIND_VIEW &&
                relation->rd_rel->relkind != RELKIND_MATVIEW &&
                relation->rd_rel->relkind != RELKIND_COMPOSITE_TYPE &&
                relation->rd_rel->relkind != RELKIND_FOREIGN_TABLE &&
                relation->rd_rel->relkind != RELKIND_PARTITIONED_TABLE) {
                ereport(ERROR, "cannot set security label on relation \"%s\"",
                       RelationGetRelationName(relation));
            }
            break;
        default:
            break;
    }

    // Let provider validate/veto the label
    provider->hook(&address, stmt->label);

    // Apply the security label
    SetSecurityLabel(&address, provider->provider_name, stmt->label);

    // Close relation if opened (but keep locks until commit)
    if (relation != NULL) {
        relation_close(relation, NoLock);
    }

    return address;
}
```