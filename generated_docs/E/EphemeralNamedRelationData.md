# EphemeralNamedRelationData

## Location
[src/include/utils/queryenvironment.h:50-54](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/queryenvironment.h#L50-L54)

## Overview
EphemeralNamedRelationData is the complete data structure for ephemeral named relations (ENRs), combining metadata with actual execution-time data access mechanisms for temporary named relations like trigger transition tables.

## Definition
```c
typedef struct EphemeralNamedRelationData
{
    EphemeralNamedRelationMetadataData md;
    void       *reldata;        /* structure for execution-time access to data */
} EphemeralNamedRelationData;
```

## Detailed Description
This structure represents the complete ephemeral named relation, building upon EphemeralNamedRelationMetadataData by adding runtime data access capabilities. ENRs are temporary named relations used primarily for parsing and accessing named relations that do not exist in the system catalogs, with the most common use case being transition tables in AFTER triggers.

The structure combines:
1. Metadata (md): All the descriptive information about the relation including name, type descriptor, and statistics
2. Runtime data (reldata): A void pointer to execution-time data structures that provide actual access to the relation's contents

This design separates the declarative aspects (metadata) from the operational aspects (data access) of ephemeral named relations.

## Parameters / Member Variables
- `md`: Embedded EphemeralNamedRelationMetadataData structure containing all metadata about the ephemeral relation including name, tuple descriptor information, type, and tuple count estimates
- `reldata`: Generic pointer to execution-time data access structure, typically pointing to a tuplestore or similar data container that holds the actual relation data during query execution

## Dependencies
- Functions called/Symbols referenced:
  - [EphemeralNamedRelationMetadataData](EphemeralNamedRelationMetadataData.md) (embedded structure containing metadata)

- Called from (representative examples):
  - EphemeralNamedRelation (typedef pointer to this structure)
  - [SPI_register_trigger_data](../S/SPI_register_trigger_data.md) (registers trigger transition table data in SPI context)
  - [register_ENR](../r/register_ENR.md) (registers an ENR in the query environment)
  - [get_ENR](../g/get_ENR.md) (retrieves an ENR from the query environment)

## Notes and Other Information
- This structure is the complete representation of an ENR, while EphemeralNamedRelationMetadataData contains only metadata
- The reldata member typically points to tuplestore structures during trigger execution
- Used extensively in the SPI (Server Programming Interface) for trigger transition tables
- The void* design for reldata allows flexibility in underlying data storage mechanisms
- Part of PostgreSQL's query environment infrastructure for handling temporary named relations
- Essential for AFTER trigger functionality where OLD and NEW transition tables must be accessible by name
- Located in src/include/utils/queryenvironment.h alongside related ENR definitions