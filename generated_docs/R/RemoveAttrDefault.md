# RemoveAttrDefault

## Location
[src/backend/catalog/pg_attrdef.c:219-273](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_attrdef.c#L219-L273)

## Overview
RemoveAttrDefault removes the default expression for a specified column from a relation, deleting the corresponding entry from the pg_attrdef catalog table.

## Definition
```c
void RemoveAttrDefault(Oid relid, AttrNumber attnum, DropBehavior behavior, bool complain, bool internal)
```

## Detailed Description
This function removes an attribute default entry from the pg_attrdef system catalog. It performs a system table scan to locate the default entry for the specified relation and attribute number combination. When found, it uses the object deletion framework (performDeletion) to remove the entry, which properly handles dependency cascading based on the specified drop behavior. The function handles the case where no default exists - it can either raise an error or return silently depending on the complain parameter. The deletion is performed within appropriate locking to ensure consistency.

## Parameters / Member Variables
- `relid`: The OID of the relation containing the column
- `attnum`: The attribute number (column number) whose default should be removed  
- `behavior`: The drop behavior (CASCADE, RESTRICT, etc.) controlling how dependencies are handled
- `complain`: Boolean indicating whether to raise an error if no default is found
- `internal`: Boolean indicating whether this is an internal deletion (affects deletion flags)

## Dependencies
- Functions called/Symbols referenced:
  - [ScanKeyInit](../S/ScanKeyInit.md): Initializes scan keys for system table scanning
  - [systable_beginscan](../s/systable_beginscan.md): Begins systematic scan of system table with index
  - [systable_getnext](../s/systable_getnext.md): Retrieves next tuple from system table scan
  - [systable_endscan](../s/systable_endscan.md): Ends system table scan
  - [performDeletion](../p/performDeletion.md): Performs object deletion with dependency handling
  - [table_open](../t/table_open.md)/table_close: Opens and closes system catalog table
  - PERFORM_DELETION_INTERNAL: Flag for internal deletion operations

- Called from (representative examples):
  - [ATExecColumnDefault](../A/ATExecColumnDefault.md): During ALTER TABLE DROP DEFAULT operations
  - [ATExecCookedColumnDefault](../A/ATExecCookedColumnDefault.md): When processing column default changes
  - [ATExecSetExpression](../A/ATExecSetExpression.md): When setting new expressions (removes old default)
  - [ATExecDropExpression](../A/ATExecDropExpression.md): When dropping column expressions
  - [ATExecAlterColumnType](../A/ATExecAlterColumnType.md): During column type changes that affect defaults

## Notes and Other Information
The function uses a systematic scan of pg_attrdef with the AttrDefaultIndexId index for efficient lookup. Although the comment indicates there should be at most one matching tuple, the implementation uses a loop to handle potential edge cases robustly. The function integrates with PostgreSQL's object deletion framework, ensuring proper dependency cascade handling. When performing internal deletions, special flags are passed to performDeletion to distinguish from user-initiated operations. The function maintains proper locking throughout the operation to ensure catalog consistency.