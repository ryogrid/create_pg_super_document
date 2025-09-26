# EphemeralNamedRelationMetadataData

## Location
src/include/utils/queryenvironment.h: 32 - 42

## Overview
EphemeralNamedRelationMetadataData is a structure that holds metadata for ephemeral named relations (ENRs) in PostgreSQL, which are temporary relations used for features like trigger transition tables that exist only during query execution.

## Definition
```c
typedef struct EphemeralNamedRelationMetadataData
{
    char       *name;           /* name used to identify the relation */

    /* only one of the next two fields should be used */
    Oid         reliddesc;      /* oid of relation to get tupdesc */
    TupleDesc   tupdesc;        /* description of result rows */

    EphemeralNameRelationType enrtype;  /* to identify type of relation */
    double      enrtuples;      /* estimated number of tuples */
} EphemeralNamedRelationMetadataData;
```

## Detailed Description
This structure serves as metadata container for ephemeral named relations in PostgreSQL. ENRs are temporary, named relations that exist only during query execution and are not stored in system catalogs. The most common use case is for trigger transition tables (OLD and NEW tables in AFTER triggers).

The structure is designed to handle two distinct scenarios:
1. ENRs that correspond to existing catalog relations (using reliddesc to reference the OID)
2. ENRs that are independent of catalog relations (storing the TupleDesc directly)

The design ensures that only one of reliddesc or tupdesc is used at any time, never both, providing flexibility while maintaining efficiency.

## Parameters / Member Variables
- `name`: A string identifier used to name and reference the ephemeral relation
- `reliddesc`: OID of an existing catalog relation whose tuple descriptor should be used (mutually exclusive with tupdesc)  
- `tupdesc`: Direct storage of tuple descriptor for relations independent of catalog entries (mutually exclusive with reliddesc)
- `enrtype`: Enumeration value specifying the type of ephemeral named relation (currently only ENR_NAMED_TUPLESTORE)
- `enrtuples`: Statistical estimate of the number of tuples expected in this relation for query planning purposes

## Dependencies
- Functions called/Symbols referenced:
  - EphemeralNameRelationType (enum defining ENR types)
  - TupleDesc (tuple descriptor structure from access/tupdesc.h)
  - Oid (object identifier type)

- Called from (representative examples):
  - EphemeralNamedRelationMetadata (typedef pointer to this structure)
  - EphemeralNamedRelationData (contains this structure as a member)

## Notes and Other Information
- This structure is part of PostgreSQL's query environment infrastructure
- The mutual exclusion between reliddesc and tupdesc is enforced by design convention rather than language constraints
- Used primarily in trigger execution contexts where transition tables need to be accessible by name
- The enrtuples field provides cardinality estimates for query optimization
- Located in src/include/utils/queryenvironment.h as part of the query environment API