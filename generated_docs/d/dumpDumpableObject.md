# dumpDumpableObject

## Location
[src/bin/pg_dump/pg_dump.c:10522-10714](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L10522-L10714)

## Overview
The central dispatcher function in pg_dump that routes each database object to its appropriate dump function based on object type.

## Definition

```c
static void
dumpDumpableObject(Archive *fout, DumpableObject *dobj)
```
## Detailed Description
The  function serves as the main dispatcher in pg_dump's dumping process. It takes a DumpableObject and routes it to the appropriate specialized dump function based on the object's type. Before dispatching, it performs optimization by clearing any dump-request bits for components that don't exist for the current object type, allowing the use of  as a safe default request for all object types.

The function contains a comprehensive switch statement that handles all possible PostgreSQL database object types, from basic objects like namespaces and types to complex objects like publications, subscriptions, and large objects. Each case calls the corresponding specialized dump function with the properly cast object pointer.

## Parameters / Member Variables
- `*fout`: Archive structure representing the dump destination and containing connection/output information
- `*dobj`: Pointer to the DumpableObject to be processed, containing object metadata and dump requirements
## Dependencies
- Functions called/Symbols referenced:
  - [dumpNamespace](dumpNamespace.md)
  - [dumpExtension](dumpExtension.md)  
  - [dumpType](dumpType.md)
  - [dumpShellType](dumpShellType.md)
  - [dumpFunc](dumpFunc.md)
  - [dumpAgg](dumpAgg.md)
  - [dumpOpr](dumpOpr.md)
  - [dumpAccessMethod](dumpAccessMethod.md)
  - [dumpOpclass](dumpOpclass.md)
  - [dumpOpfamily](dumpOpfamily.md)
  - [dumpCollation](dumpCollation.md)
  - [dumpConversion](dumpConversion.md)
  - [dumpTable](dumpTable.md)
  - [dumpTableAttach](dumpTableAttach.md)
  - [dumpAttrDef](dumpAttrDef.md)
  - [dumpIndex](dumpIndex.md)
  - [dumpIndexAttach](dumpIndexAttach.md)
  - [dumpStatisticsExt](dumpStatisticsExt.md)
  - [refreshMatViewData](../r/refreshMatViewData.md)
  - [dumpRule](dumpRule.md)
  - [dumpTrigger](dumpTrigger.md)
  - [dumpEventTrigger](dumpEventTrigger.md)
  - [dumpConstraint](dumpConstraint.md)
  - [dumpProcLang](dumpProcLang.md)
  - [dumpCast](dumpCast.md)
  - [dumpTransform](dumpTransform.md)
  - [dumpSequenceData](dumpSequenceData.md)
  - [dumpTableData](dumpTableData.md)
  - [dumpTSParser](dumpTSParser.md)
  - [dumpTSDictionary](dumpTSDictionary.md)
  - [dumpTSTemplate](dumpTSTemplate.md)
  - [dumpTSConfig](dumpTSConfig.md)
  - [dumpForeignDataWrapper](dumpForeignDataWrapper.md)
  - [dumpForeignServer](dumpForeignServer.md)
  - [dumpDefaultACL](dumpDefaultACL.md)
  - [dumpLO](dumpLO.md)
  - [dumpPolicy](dumpPolicy.md)
  - [dumpPublication](dumpPublication.md)
  - [dumpPublicationTable](dumpPublicationTable.md)
  - [dumpPublicationNamespace](dumpPublicationNamespace.md)
  - [dumpSubscription](dumpSubscription.md)
  - [dumpSubscriptionTable](dumpSubscriptionTable.md)
  - [findObjectByDumpId](../f/findObjectByDumpId.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpLOs](dumpLOs.md)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_dump.c:1036)

## Notes and Other Information
- Performs component filtering optimization before dispatching to specialized dump functions
- Contains special handling for large object data with size estimation for parallel dump ordering
- Some object types like , , and  are never dumped
- The function supports over 30 different PostgreSQL object types
- Early return optimization when no dump components are requested ()