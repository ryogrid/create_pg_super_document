# OperatorShellMake

## Location
[src/backend/catalog/pg_operator.c:193-288](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_operator.c#L193-L288)

## Overview
Creates a "shell" entry in the pg_operator catalog for an operator that will be defined later, enabling forward references in operator definitions.

## Definition
```c
static Oid OperatorShellMake(const char *operatorName, Oid operatorNamespace, Oid leftTypeId, Oid rightTypeId)
```

## Detailed Description
This function creates a placeholder operator entry in the system catalog that can be referenced before the operator is fully defined. Shell operators are essential for handling circular dependencies between operators, such as when operators need to reference each other or when an operator's implementation function hasn't been created yet.

The function performs validation of the operator name, opens the pg_operator system catalog, and creates a new tuple with most fields set to InvalidOid (indicating they're not yet defined). The key distinguishing feature of a shell operator is that oprcode is set to InvalidOid, which marks it as incomplete.

## Parameters / Member Variables
- `operatorName`: The name of the operator to create a shell for
- `operatorNamespace`: The namespace (schema) where the operator will reside
- `leftTypeId`: The OID of the left operand type (InvalidOid for prefix operators)
- `rightTypeId`: The OID of the right operand type

## Dependencies
- Functions called/Symbols referenced:
  - validOperatorName
  - table_open
  - GetNewOidWithIndex
  - namestrcpy
  - heap_form_tuple
  - CatalogTupleInsert
  - makeOperatorDependencies
  - InvokeObjectPostCreateHook
  - CommandCounterIncrement
  - table_close
- Called from:
  - OperatorCreate (when forward references are needed)

## Notes and Other Information
- This is a static function, only accessible within pg_operator.c
- Shell operators must be completed later with OperatorCreate
- The function sets oprcode to InvalidOid to mark the operator as a shell
- Creates proper dependencies and triggers post-creation hooks
- Uses CommandCounterIncrement to make the new tuple visible for subsequent operations
- Essential for resolving circular dependencies in operator definitions

## Simplified Source

```c
static Oid
OperatorShellMake(const char *operatorName,
                  Oid operatorNamespace,
                  Oid leftTypeId,
                  Oid rightTypeId)
{
    Relation    pg_operator_desc;
    Oid         operatorObjectId;
    int         i;
    HeapTuple   tup;
    Datum       values[Natts_pg_operator];
    bool        nulls[Natts_pg_operator];
    NameData    oname;
    TupleDesc   tupDesc;

    // Validate operator name
    if (!validOperatorName(operatorName))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_NAME),
                 errmsg("\"%s\" is not a valid operator name",
                        operatorName)));

    // Open pg_operator catalog
    pg_operator_desc = table_open(OperatorRelationId, RowExclusiveLock);
    tupDesc = pg_operator_desc->rd_att;

    // Initialize arrays
    for (i = 0; i < Natts_pg_operator; ++i)
    {
        nulls[i] = false;
        values[i] = (Datum) NULL;
    }

    // Set up shell operator values (oprcode = InvalidOid marks it as shell)
    operatorObjectId = GetNewOidWithIndex(pg_operator_desc, OperatorOidIndexId,
                                          Anum_pg_operator_oid);
    values[Anum_pg_operator_oid - 1] = ObjectIdGetDatum(operatorObjectId);
    namestrcpy(&oname, operatorName);
    values[Anum_pg_operator_oprname - 1] = NameGetDatum(&oname);
    values[Anum_pg_operator_oprnamespace - 1] = ObjectIdGetDatum(operatorNamespace);
    values[Anum_pg_operator_oprowner - 1] = ObjectIdGetDatum(GetUserId());
    values[Anum_pg_operator_oprkind - 1] = CharGetDatum(leftTypeId ? 'b' : 'l');
    values[Anum_pg_operator_oprcanmerge - 1] = BoolGetDatum(false);
    values[Anum_pg_operator_oprcanhash - 1] = BoolGetDatum(false);
    values[Anum_pg_operator_oprleft - 1] = ObjectIdGetDatum(leftTypeId);
    values[Anum_pg_operator_oprright - 1] = ObjectIdGetDatum(rightTypeId);
    values[Anum_pg_operator_oprresult - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_operator_oprcom - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_operator_oprnegate - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_operator_oprcode - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_operator_oprrest - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_operator_oprjoin - 1] = ObjectIdGetDatum(InvalidOid);

    // Create and insert the shell operator tuple
    tup = heap_form_tuple(tupDesc, values, nulls);
    CatalogTupleInsert(pg_operator_desc, tup);

    // Add dependencies and trigger hooks
    makeOperatorDependencies(tup, true, false);
    heap_freetuple(tup);
    InvokeObjectPostCreateHook(OperatorRelationId, operatorObjectId, 0);

    // Make tuple visible and close catalog
    CommandCounterIncrement();
    table_close(pg_operator_desc, RowExclusiveLock);

    return operatorObjectId;
}
```