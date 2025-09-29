# CreateConstraintEntry

## Location
[src/backend/catalog/pg_constraint.c:48-398](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_constraint.c#L48-L398)

## Overview
Creates a constraint table entry in the pg_constraint catalog and establishes necessary dependency relationships, but does not create subsidiary records like triggers or indexes.

## Definition

```c
struct_array_builtin(conkey, constraintNKeys, INT2OID);
```
## Detailed Description
This function is the core mechanism for creating constraint entries in PostgreSQL's pg_constraint system catalog. It handles all types of constraints including CHECK, PRIMARY KEY, UNIQUE, FOREIGN KEY, EXCLUSION, and domain constraints. The function converts C arrays to PostgreSQL arrays, populates all catalog fields, and establishes both automatic and normal dependency relationships. It does not create subsidiary objects like indexes or triggers - those are handled separately by calling code.

## Parameters / Member Variables
- : Name of the constraint to be created
- : OID of the namespace containing the constraint
- : Single character indicating constraint type (c=CHECK, p=PRIMARY KEY, u=UNIQUE, f=FOREIGN KEY, x=EXCLUSION, t=trigger constraint, n=NOT NULL)
- : Whether the constraint can be deferred
- : Whether the constraint is initially deferred
- : Whether the constraint has been validated
- : OID of parent constraint (for inherited constraints)
- : OID of the relation the constraint applies to
- : Array of column numbers that the constraint applies to
- : Number of key columns in constraintKey array
- : Total number of key columns including those inherited
- : OID of domain for domain constraints
- : OID of supporting index for UNIQUE/PRIMARY KEY/EXCLUSION constraints
- : OID of referenced relation for FOREIGN KEY constraints
- : Array of column numbers in the foreign key
- : Array of PK/FK equality operator OIDs
- : Array of PK/PK equality operator OIDs
- : Array of FK/FK equality operator OIDs
- : Number of foreign key columns
- : Foreign key UPDATE action (r=RESTRICT, c=CASCADE, n=SET NULL, d=SET DEFAULT, a=NO ACTION)
- : Foreign key DELETE action (same codes as UPDATE)
- : Columns to set for SET NULL/DEFAULT on delete
- : Number of columns in fkDeleteSetCols
- : Foreign key match type (f=FULL, p=PARTIAL, s=SIMPLE)
- : Array of exclusion operator OIDs for EXCLUSION constraints
- : Parsed CHECK constraint expression
- : Binary representation of CHECK constraint expression
- : Whether constraint is locally defined (not inherited)
- : Number of direct inheritance ancestors that also have this constraint
- : Whether constraint should not be inherited by child tables
- : Whether this is an internal system-generated constraint

## Dependencies
- Functions called/Symbols referenced:
  - [namestrcpy](../n/namestrcpy.md)
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md)
  - [construct_array_builtin](../c/construct_array_builtin.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](CatalogTupleInsert.md)
  - ObjectAddressSet
  - [new_object_addresses](../n/new_object_addresses.md)
  - [add_exact_object_address](../a/add_exact_object_address.md)
  - [record_object_address_dependencies](../r/record_object_address_dependencies.md)
  - [recordDependencyOnSingleRelExpr](../r/recordDependencyOnSingleRelExpr.md)
  - InvokeObjectPostCreateHookArg
- Called from (representative examples):
  - [StoreRelCheck](../S/StoreRelCheck.md)
  - [index_constraint_create](../i/index_constraint_create.md)
  - [addFkConstraint](../a/addFkConstraint.md)
  - [domainAddCheckConstraint](../d/domainAddCheckConstraint.md)
  - [domainAddNotNullConstraint](../d/domainAddNotNullConstraint.md)

## Notes and Other Information
- Returns the OID of the newly created constraint entry
- Handles conversion of C arrays to PostgreSQL array types for storage in the catalog
- Establishes AUTO dependencies to owning relations/domains and NORMAL dependencies to referenced objects
- Does not validate constraint data - validation is handled elsewhere
- Thread-safe as it uses proper catalog locking mechanisms

## Simplified Source

```c
Oid CreateConstraintEntry(const char *constraintName,
                         Oid constraintNamespace, char constraintType,
                         bool isDeferrable, bool isDeferred, bool isValidated,
                         Oid parentConstrId, Oid relId, const int16 *constraintKey,
                         int constraintNKeys, int constraintNTotalKeys,
                         Oid domainId, Oid indexRelId, Oid foreignRelId,
                         const int16 *foreignKey, const Oid *pfEqOp,
                         const Oid *ppEqOp, const Oid *ffEqOp, int foreignNKeys,
                         char foreignUpdateType, char foreignDeleteType,
                         const int16 *fkDeleteSetCols, int numFkDeleteSetCols,
                         char foreignMatchType, const Oid *exclOp,
                         Node *conExpr, const char *conBin,
                         bool conIsLocal, int conInhCount, bool conNoInherit,
                         bool is_internal)
{
    Relation conDesc;
    Oid conOid;
    HeapTuple tup;
    bool nulls[Natts_pg_constraint];
    Datum values[Natts_pg_constraint];
    NameData cname;

    // Open constraint catalog
    conDesc = table_open(ConstraintRelationId, RowExclusiveLock);

    // Copy constraint name
    namestrcpy(&cname, constraintName);

    // Convert C arrays to PostgreSQL arrays
    ArrayType *conkeyArray = NULL;
    if (constraintNKeys > 0) {
        Datum *conkey = palloc(constraintNKeys * sizeof(Datum));
        for (int i = 0; i < constraintNKeys; i++)
            conkey[i] = Int16GetDatum(constraintKey[i]);
        conkeyArray = construct_array_builtin(conkey, constraintNKeys, INT2OID);
    }

    // Handle foreign key arrays
    ArrayType *confkeyArray = NULL, *conpfeqopArray = NULL;
    ArrayType *conppeqopArray = NULL, *conffeqopArray = NULL;
    ArrayType *confdelsetcolsArray = NULL;

    if (foreignNKeys > 0) {
        Datum *fkdatums = palloc(Max(foreignNKeys, numFkDeleteSetCols) * sizeof(Datum));

        // Foreign key columns
        for (int i = 0; i < foreignNKeys; i++)
            fkdatums[i] = Int16GetDatum(foreignKey[i]);
        confkeyArray = construct_array_builtin(fkdatums, foreignNKeys, INT2OID);

        // Equality operators
        for (int i = 0; i < foreignNKeys; i++)
            fkdatums[i] = ObjectIdGetDatum(pfEqOp[i]);
        conpfeqopArray = construct_array_builtin(fkdatums, foreignNKeys, OIDOID);

        // Similar for ppEqOp and ffEqOp arrays...
        // Delete SET columns if any
        if (numFkDeleteSetCols > 0) {
            for (int i = 0; i < numFkDeleteSetCols; i++)
                fkdatums[i] = Int16GetDatum(fkDeleteSetCols[i]);
            confdelsetcolsArray = construct_array_builtin(fkdatums, numFkDeleteSetCols, INT2OID);
        }
    }

    // Handle exclusion operators
    ArrayType *conexclopArray = NULL;
    if (exclOp != NULL) {
        Datum *opdatums = palloc(constraintNKeys * sizeof(Datum));
        for (int i = 0; i < constraintNKeys; i++)
            opdatums[i] = ObjectIdGetDatum(exclOp[i]);
        conexclopArray = construct_array_builtin(opdatums, constraintNKeys, OIDOID);
    }

    // Initialize values array
    for (int i = 0; i < Natts_pg_constraint; i++) {
        nulls[i] = false;
        values[i] = (Datum) NULL;
    }

    // Get new OID and populate basic fields
    conOid = GetNewOidWithIndex(conDesc, ConstraintOidIndexId, Anum_pg_constraint_oid);
    values[Anum_pg_constraint_oid - 1] = ObjectIdGetDatum(conOid);
    values[Anum_pg_constraint_conname - 1] = NameGetDatum(&cname);
    values[Anum_pg_constraint_connamespace - 1] = ObjectIdGetDatum(constraintNamespace);
    values[Anum_pg_constraint_contype - 1] = CharGetDatum(constraintType);
    values[Anum_pg_constraint_condeferrable - 1] = BoolGetDatum(isDeferrable);
    values[Anum_pg_constraint_condeferred - 1] = BoolGetDatum(isDeferred);
    values[Anum_pg_constraint_convalidated - 1] = BoolGetDatum(isValidated);
    values[Anum_pg_constraint_conrelid - 1] = ObjectIdGetDatum(relId);
    values[Anum_pg_constraint_contypid - 1] = ObjectIdGetDatum(domainId);
    values[Anum_pg_constraint_conindid - 1] = ObjectIdGetDatum(indexRelId);
    values[Anum_pg_constraint_conparentid - 1] = ObjectIdGetDatum(parentConstrId);
    values[Anum_pg_constraint_confrelid - 1] = ObjectIdGetDatum(foreignRelId);

    // Set array fields (with proper null handling)
    if (conkeyArray)
        values[Anum_pg_constraint_conkey - 1] = PointerGetDatum(conkeyArray);
    else
        nulls[Anum_pg_constraint_conkey - 1] = true;

    // Similar for other arrays...

    // Create and insert tuple
    tup = heap_form_tuple(RelationGetDescr(conDesc), values, nulls);
    CatalogTupleInsert(conDesc, tup);

    table_close(conDesc, RowExclusiveLock);

    // Record dependencies
    ObjectAddress conobject;
    ObjectAddressSet(conobject, ConstraintRelationId, conOid);

    // Auto dependencies (to owning relation/domain)
    ObjectAddresses *addrs_auto = new_object_addresses();
    if (OidIsValid(relId)) {
        ObjectAddress relobject;
        ObjectAddressSet(relobject, RelationRelationId, relId);
        add_exact_object_address(&relobject, addrs_auto);
    }
    record_object_address_dependencies(&conobject, addrs_auto, DEPENDENCY_AUTO);

    // Normal dependencies (to referenced objects)
    ObjectAddresses *addrs_normal = new_object_addresses();
    if (OidIsValid(foreignRelId)) {
        ObjectAddress relobject;
        ObjectAddressSet(relobject, RelationRelationId, foreignRelId);
        add_exact_object_address(&relobject, addrs_normal);
    }
    record_object_address_dependencies(&conobject, addrs_normal, DEPENDENCY_NORMAL);

    // Record expression dependencies if CHECK constraint
    if (conExpr != NULL)
        recordDependencyOnSingleRelExpr(&conobject, conExpr, relId,
                                       DEPENDENCY_NORMAL, DEPENDENCY_NORMAL, false);

    // Post-creation hook
    InvokeObjectPostCreateHookArg(ConstraintRelationId, conOid, 0, is_internal);

    return conOid;
}
```