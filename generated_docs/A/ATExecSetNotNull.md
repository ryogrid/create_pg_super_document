# ATExecSetNotNull

## Location
[src/backend/commands/tablecmds.c:7760-7841](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L7760-L7841)

## Overview
ATExecSetNotNull is the execution phase function for the ALTER TABLE ALTER COLUMN SET NOT NULL command, which actually modifies the catalog to mark a column as NOT NULL and determines if data validation is needed.

## Definition

```c
static ObjectAddress
ATExecSetNotNull(AlteredTableInfo *tab, Relation rel,
				 const char *colName, LOCKMODE lockmode)
```
## Detailed Description
This function performs the actual catalog modification to set a column's NOT NULL constraint during ALTER TABLE operations. The function operates in several phases:

1. **Column Lookup**: Uses the system catalog to find the specified column and validates its existence and type (preventing modification of system columns).

2. **Catalog Modification**: If the column is not already NOT NULL, it updates the pg_attribute catalog to set the attnotnull flag to true.

3. **Validation Optimization**: Checks if existing constraints already guarantee that the column contains no NULL values using NotNullImpliedByRelConstraints(). If no such constraint exists, it flags that Phase 3 validation is required.

4. **Post-Alter Processing**: Invokes post-alter hooks and returns the object address of the modified column, or InvalidObjectAddress if no change was made.

The function is designed to be efficient by skipping unnecessary validation when existing constraints already ensure NOT NULL semantics.

## Parameters / Member Variables
- `*tab`: AlteredTableInfo structure containing information about the table being altered and tracking validation requirements
- `rel`: The relation being modified
- `*colName`: Name of the column to set as NOT NULL
- `lockmode`: Lock mode for accessing the relation (currently unused in this function)
## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCacheCopyAttName](../S/SearchSysCacheCopyAttName.md) (to lookup column in system catalog)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md) (to update the pg_attribute catalog)
  - [NotNullImpliedByRelConstraints](../N/NotNullImpliedByRelConstraints.md) (to check if existing constraints guarantee NOT NULL)
  - ObjectAddressSubSet (to create return address for the modified column)
  - InvokeObjectPostAlterHook (to trigger post-alter processing)
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md) (main ALTER TABLE command execution dispatcher)

## Notes and Other Information
- Returns InvalidObjectAddress if the column was already NOT NULL, indicating no change was made
- The function integrates with PostgreSQL's three-phase ALTER TABLE processing: preparation, execution, and validation
- Phase 3 validation (checking for existing NULL values) is only scheduled if no existing constraint can prove the column is already NULL-free
- System columns (attnum <= 0) cannot be altered and will generate an error
- The function uses RowExclusiveLock on the attribute relation to ensure safe concurrent access during catalog updates

## Simplified Source

```c
static ObjectAddress
ATExecSetNotNull(AlteredTableInfo *tab, Relation rel,
                 const char *colName, LOCKMODE lockmode)
{
    HeapTuple tuple;
    AttrNumber attnum;
    Relation attr_rel;
    ObjectAddress address;

    // Open attribute relation and lookup the column
    attr_rel = table_open(AttributeRelationId, RowExclusiveLock);
    tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), colName);

    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, "column does not exist");

    attnum = ((Form_pg_attribute) GETSTRUCT(tuple))->attnum;

    // Prevent altering system columns
    if (attnum <= 0)
        ereport(ERROR, "cannot alter system column");

    // Update catalog if column is not already NOT NULL
    if (!((Form_pg_attribute) GETSTRUCT(tuple))->attnotnull)
    {
        ((Form_pg_attribute) GETSTRUCT(tuple))->attnotnull = true;
        CatalogTupleUpdate(attr_rel, &tuple->t_self, tuple);

        // Check if we need to verify existing data for NULLs
        // Skip verification if existing constraints already guarantee NOT NULL
        if (!tab->verify_new_notnull &&
            !NotNullImpliedByRelConstraints(rel, (Form_pg_attribute) GETSTRUCT(tuple)))
        {
            tab->verify_new_notnull = true;  // Tell Phase 3 to verify constraint
        }

        ObjectAddressSubSet(address, RelationRelationId, RelationGetRelid(rel), attnum);
    }
    else
        address = InvalidObjectAddress;  // Already NOT NULL, no change needed

    // Cleanup and post-alter processing
    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), attnum);
    table_close(attr_rel, RowExclusiveLock);

    return address;
}
```