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

## Simplified Source

```c
static void dumpDumpableObject(Archive *fout, DumpableObject *dobj) {
    // Clear non-existent component bits for optimization
    dobj->dump &= dobj->components;

    // Early return if nothing to dump
    if (dobj->dump == 0)
        return;

    // Dispatch to appropriate dump function based on object type
    switch (dobj->objType) {
        case DO_NAMESPACE:
            dumpNamespace(fout, (const NamespaceInfo *) dobj);
            break;
        case DO_EXTENSION:
            dumpExtension(fout, (const ExtensionInfo *) dobj);
            break;
        case DO_TYPE:
            dumpType(fout, (const TypeInfo *) dobj);
            break;
        case DO_SHELL_TYPE:
            dumpShellType(fout, (const ShellTypeInfo *) dobj);
            break;
        case DO_FUNC:
            dumpFunc(fout, (const FuncInfo *) dobj);
            break;
        case DO_AGG:
            dumpAgg(fout, (const AggInfo *) dobj);
            break;
        case DO_OPERATOR:
            dumpOpr(fout, (const OprInfo *) dobj);
            break;
        case DO_ACCESS_METHOD:
            dumpAccessMethod(fout, (const AccessMethodInfo *) dobj);
            break;
        case DO_OPCLASS:
            dumpOpclass(fout, (const OpclassInfo *) dobj);
            break;
        case DO_OPFAMILY:
            dumpOpfamily(fout, (const OpfamilyInfo *) dobj);
            break;
        case DO_COLLATION:
            dumpCollation(fout, (const CollInfo *) dobj);
            break;
        case DO_CONVERSION:
            dumpConversion(fout, (const ConvInfo *) dobj);
            break;
        case DO_TABLE:
            dumpTable(fout, (const TableInfo *) dobj);
            break;
        case DO_TABLE_ATTACH:
            dumpTableAttach(fout, (const TableAttachInfo *) dobj);
            break;
        case DO_ATTRDEF:
            dumpAttrDef(fout, (const AttrDefInfo *) dobj);
            break;
        case DO_INDEX:
            dumpIndex(fout, (const IndxInfo *) dobj);
            break;
        case DO_INDEX_ATTACH:
            dumpIndexAttach(fout, (const IndexAttachInfo *) dobj);
            break;
        case DO_STATSEXT:
            dumpStatisticsExt(fout, (const StatsExtInfo *) dobj);
            break;
        case DO_REFRESH_MATVIEW:
            refreshMatViewData(fout, (const TableDataInfo *) dobj);
            break;
        case DO_RULE:
            dumpRule(fout, (const RuleInfo *) dobj);
            break;
        case DO_TRIGGER:
            dumpTrigger(fout, (const TriggerInfo *) dobj);
            break;
        case DO_EVENT_TRIGGER:
            dumpEventTrigger(fout, (const EventTriggerInfo *) dobj);
            break;
        case DO_CONSTRAINT:
        case DO_FK_CONSTRAINT:
            dumpConstraint(fout, (const ConstraintInfo *) dobj);
            break;
        case DO_PROCLANG:
            dumpProcLang(fout, (const ProcLangInfo *) dobj);
            break;
        case DO_CAST:
            dumpCast(fout, (const CastInfo *) dobj);
            break;
        case DO_TRANSFORM:
            dumpTransform(fout, (const TransformInfo *) dobj);
            break;
        case DO_SEQUENCE_SET:
            dumpSequenceData(fout, (const TableDataInfo *) dobj);
            break;
        case DO_TABLE_DATA:
            dumpTableData(fout, (const TableDataInfo *) dobj);
            break;
        case DO_DUMMY_TYPE:
            // Table rowtypes and array types are never dumped separately
            break;
        case DO_TSPARSER:
            dumpTSParser(fout, (const TSParserInfo *) dobj);
            break;
        case DO_TSDICT:
            dumpTSDictionary(fout, (const TSDictInfo *) dobj);
            break;
        case DO_TSTEMPLATE:
            dumpTSTemplate(fout, (const TSTemplateInfo *) dobj);
            break;
        case DO_TSCONFIG:
            dumpTSConfig(fout, (const TSConfigInfo *) dobj);
            break;
        case DO_FDW:
            dumpForeignDataWrapper(fout, (const FdwInfo *) dobj);
            break;
        case DO_FOREIGN_SERVER:
            dumpForeignServer(fout, (const ForeignServerInfo *) dobj);
            break;
        case DO_DEFAULT_ACL:
            dumpDefaultACL(fout, (const DefaultACLInfo *) dobj);
            break;
        case DO_LARGE_OBJECT:
            dumpLO(fout, (const LoInfo *) dobj);
            break;
        case DO_LARGE_OBJECT_DATA:
            if (dobj->dump & DUMP_COMPONENT_DATA) {
                LoInfo *loinfo = (LoInfo *) findObjectByDumpId(dobj->dependencies[0]);
                if (loinfo == NULL)
                    pg_fatal("missing metadata for large objects \"%s\"", dobj->name);

                TocEntry *te = ArchiveEntry(fout, dobj->catId, dobj->dumpId,
                                            ARCHIVE_OPTS(.tag = dobj->name,
                                                         .owner = loinfo->rolname,
                                                         .description = "BLOBS",
                                                         .section = SECTION_DATA,
                                                         .deps = dobj->dependencies,
                                                         .nDeps = dobj->nDeps,
                                                         .dumpFn = dumpLOs,
                                                         .dumpArg = loinfo));

                // Set size estimate for parallel dump ordering (8K per blob)
                te->dataLength = loinfo->numlos * (pgoff_t) 8192;
            }
            break;
        case DO_POLICY:
            dumpPolicy(fout, (const PolicyInfo *) dobj);
            break;
        case DO_PUBLICATION:
            dumpPublication(fout, (const PublicationInfo *) dobj);
            break;
        case DO_PUBLICATION_REL:
            dumpPublicationTable(fout, (const PublicationRelInfo *) dobj);
            break;
        case DO_PUBLICATION_TABLE_IN_SCHEMA:
            dumpPublicationNamespace(fout, (const PublicationSchemaInfo *) dobj);
            break;
        case DO_SUBSCRIPTION:
            dumpSubscription(fout, (const SubscriptionInfo *) dobj);
            break;
        case DO_SUBSCRIPTION_REL:
            dumpSubscriptionTable(fout, (const SubRelInfo *) dobj);
            break;
        case DO_PRE_DATA_BOUNDARY:
        case DO_POST_DATA_BOUNDARY:
            // Never dumped, nothing to do
            break;
    }
}
```