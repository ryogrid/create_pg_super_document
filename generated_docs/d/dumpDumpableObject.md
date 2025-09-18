# dumpDumpableObject

## Location
src/bin/pg_dump/pg_dump.c: 10522 - 10714

## Overview
The central dispatcher function in pg_dump that routes each database object to its appropriate dump function based on object type.

## Definition


## Detailed Description
The  function serves as the main dispatcher in pg_dump's dumping process. It takes a DumpableObject and routes it to the appropriate specialized dump function based on the object's type. Before dispatching, it performs optimization by clearing any dump-request bits for components that don't exist for the current object type, allowing the use of  as a safe default request for all object types.

The function contains a comprehensive switch statement that handles all possible PostgreSQL database object types, from basic objects like namespaces and types to complex objects like publications, subscriptions, and large objects. Each case calls the corresponding specialized dump function with the properly cast object pointer.

## Parameters / Member Variables
- : Archive structure representing the dump destination and containing connection/output information
- : Pointer to the DumpableObject to be processed, containing object metadata and dump requirements

## Dependencies
- Functions called/Symbols referenced:
  - dumpNamespace
  - dumpExtension  
  - dumpType
  - dumpShellType
  - dumpFunc
  - dumpAgg
  - dumpOpr
  - dumpAccessMethod
  - dumpOpclass
  - dumpOpfamily
  - dumpCollation
  - dumpConversion
  - dumpTable
  - dumpTableAttach
  - dumpAttrDef
  - dumpIndex
  - dumpIndexAttach
  - dumpStatisticsExt
  - refreshMatViewData
  - dumpRule
  - dumpTrigger
  - dumpEventTrigger
  - dumpConstraint
  - dumpProcLang
  - dumpCast
  - dumpTransform
  - dumpSequenceData
  - dumpTableData
  - dumpTSParser
  - dumpTSDictionary
  - dumpTSTemplate
  - dumpTSConfig
  - dumpForeignDataWrapper
  - dumpForeignServer
  - dumpDefaultACL
  - dumpLO
  - dumpPolicy
  - dumpPublication
  - dumpPublicationTable
  - dumpPublicationNamespace
  - dumpSubscription
  - dumpSubscriptionTable
  - findObjectByDumpId
  - ArchiveEntry
  - dumpLOs
- Called from (representative examples):
  - main (in pg_dump.c:1036)

## Notes and Other Information
- Performs component filtering optimization before dispatching to specialized dump functions
- Contains special handling for large object data with size estimation for parallel dump ordering
- Some object types like , , and  are never dumped
- The function supports over 30 different PostgreSQL object types
- Early return optimization when no dump components are requested ()