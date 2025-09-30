# IndexSupportInitialize

## Location
[src/backend/utils/cache/relcache.c:1597-1647](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L1597-L1647)

## Overview
IndexSupportInitialize initializes an index's cached operator class information by looking up support procedures, operator families, and input types for each index attribute.

## Definition
```c
static void IndexSupportInitialize(oidvector *indclass,
                                   RegProcedure *indexSupport,
                                   Oid *opFamily,
                                   Oid *opcInType,
                                   StrategyNumber maxSupportNumber,
                                   AttrNumber maxAttributeNumber)
```

## Detailed Description
This static function initializes operator class information for an index by iterating through each attribute and looking up its corresponding operator class details. For each attribute, it calls LookupOpclassInfo() to retrieve cached operator class information and copies the support procedures, operator family OID, and operator class input type into the provided arrays. The function validates that operator class OIDs are valid and copies support procedure arrays when the access method requires support functions.

## Parameters / Member Variables
- `indclass`: oidvector containing the operator class OIDs from pg_index.indclass
- `indexSupport`: Array to store support procedure OIDs, allocated by caller
- `opFamily`: Array to store operator family OIDs for each attribute
- `opcInType`: Array to store operator class input types for each attribute  
- `maxSupportNumber`: Maximum number of support procedures per attribute (from access method)
- `maxAttributeNumber`: Number of key attributes in the index

## Dependencies
- Functions called/Symbols referenced:
  - [LookupOpclassInfo](../L/LookupOpclassInfo.md)
  - memcpy
  - elog
  - OidIsValid
  - oidvector, RegProcedure, StrategyNumber, AttrNumber, OpClassCacheEnt (types)
- Called from:
  - [RelationInitIndexAccessInfo](../R/RelationInitIndexAccessInfo.md)

## Notes and Other Information
- This is a static function within relcache.c used during index relation cache initialization
- Uses a caching mechanism through LookupOpclassInfo to avoid repeated catalog lookups
- Support procedure arrays are laid out with maxSupportNumber entries per attribute
- Only copies support procedures if maxSupportNumber > 0 (some access methods don't use support functions)
- Validates operator class OIDs to detect corrupted pg_index entries
- Part of the index access method initialization infrastructure in PostgreSQL's relation cache system

## Simplified Source

```c
static void IndexSupportInitialize(oidvector *indclass,
                                   RegProcedure *indexSupport,
                                   Oid *opFamily,
                                   Oid *opcInType,
                                   StrategyNumber maxSupportNumber,
                                   AttrNumber maxAttributeNumber) {
    // Initialize operator class info for each index attribute
    for (int attIndex = 0; attIndex < maxAttributeNumber; attIndex++) {
        // Validate operator class OID
        if (!OidIsValid(indclass->values[attIndex])) {
            elog(ERROR, "bogus pg_index tuple");
        }

        // Look up cached operator class information
        OpClassCacheEnt *opcentry = LookupOpclassInfo(indclass->values[attIndex],
                                                       maxSupportNumber);

        // Copy operator family and input type
        opFamily[attIndex] = opcentry->opcfamily;
        opcInType[attIndex] = opcentry->opcintype;

        // Copy support procedures if access method uses them
        if (maxSupportNumber > 0) {
            memcpy(&indexSupport[attIndex * maxSupportNumber],
                   opcentry->supportProcs,
                   maxSupportNumber * sizeof(RegProcedure));
        }
    }
}
```