# makeRangeConstructors

## Location
[src/backend/commands/typecmds.c:1737-1810](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L1737-L1810)

## Overview
makeRangeConstructors creates constructor functions for a newly defined range type, generating multiple overloaded functions with different argument counts for user convenience.

## Definition

```c
static void
makeRangeConstructors(const char *name, Oid namespace,
					  Oid rangeOid, Oid subtype)
```
## Detailed Description
This function creates constructor functions that allow users to create range values using the range type name as a function. Since PostgreSQL cannot determine range types uniquely from their subtype (multiple range types may share the same subtype), polymorphic constructors are not feasible, requiring explicit constructor generation for each range type.

The function creates exactly 2 constructor functions:
1. **2-argument constructor**:  - creates range with lower and upper bounds
2. **3-argument constructor**:  - creates range with bounds and flags

Each constructor is created as an internal language function with the same name as the range type, returning the range type, and marked as immutable and parallel-safe. The functions are set up with DEPENDENCY_INTERNAL relationships to the range type, ensuring they are automatically dropped when the range type is removed.

## Parameters / Member Variables
- `*name`: The name of the range type, which will also be used as the constructor function name
- `namespace`: The namespace OID where the constructor functions should be created
- `rangeOid`: The OID of the range type that the constructors will return
- `subtype`: The OID of the range's subtype, used as the parameter types for the constructors
## Dependencies
- Functions called/Symbols referenced:
  - [ProcedureCreate](../P/ProcedureCreate.md) (creates the constructor function catalog entries)
  - [buildoidvector](../b/buildoidvector.md) (constructs parameter type vectors)
  - [recordDependencyOn](../r/recordDependencyOn.md) (establishes dependency relationships)
  - DEPENDENCY_INTERNAL (dependency type constant)
  - PROKIND_FUNCTION, PROVOLATILE_IMMUTABLE, PROPARALLEL_SAFE (function attribute constants)
- Called from (representative examples):
  - [DefineRange](../D/DefineRange.md) (during range type creation)
  - AlterTypeRecurseParams (during type alterations)

## Notes and Other Information
- Creates exactly 2 overloaded constructor functions (not 4 as mentioned in the comment)
- Constructor functions have the same name as the range type for intuitive usage
- Uses internal language functions 'range_constructor2' and 'range_constructor3' as implementation
- Functions are marked as immutable since range construction is deterministic
- DEPENDENCY_INTERNAL ensures pg_dump skips these auto-generated constructors
- All constructors are owned by the bootstrap superuser and marked parallel-safe

## Simplified Source

```c
static void makeRangeConstructors(const char *name, Oid namespace,
                                 Oid rangeOid, Oid subtype) {
    static const char *const prosrc[2] = {"range_constructor2", "range_constructor3"};
    static const int pronargs[2] = {2, 3};

    Oid constructorArgTypes[3];
    ObjectAddress myself, referenced;
    int i;

    // Set up argument types: subtype, subtype, text (for bounds flags)
    constructorArgTypes[0] = subtype;
    constructorArgTypes[1] = subtype;
    constructorArgTypes[2] = TEXTOID;

    // Set up dependency target (the range type)
    referenced.classId = TypeRelationId;
    referenced.objectId = rangeOid;
    referenced.objectSubId = 0;

    // Create 2-arg and 3-arg constructors
    for (i = 0; i < lengthof(prosrc); i++) {
        oidvector *constructorArgTypesVector = buildoidvector(constructorArgTypes, pronargs[i]);

        myself = ProcedureCreate(name, namespace, false, false, rangeOid,
                                BOOTSTRAP_SUPERUSERID, INTERNALlanguageId,
                                F_FMGR_INTERNAL_VALIDATOR, prosrc[i],
                                /* standard function attributes */);

        // Make constructor depend on the range type
        recordDependencyOn(&myself, &referenced, DEPENDENCY_INTERNAL);
    }
}
```