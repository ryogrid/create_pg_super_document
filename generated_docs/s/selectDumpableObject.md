# selectDumpableObject

## Location
[src/bin/pg_dump/pg_dump.c:2144-2165](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L2144-L2165)

## Overview
A generic policy-setting function that determines whether any dumpable object should be dumped based on namespace membership or global dump settings.

## Definition

```c
static void
selectDumpableObject(DumpableObject *dobj, Archive *fout)
```
## Detailed Description
This function serves as the default policy-setting routine for database objects that don't have specialized dumping logic. It implements a two-tier decision process: objects associated with a namespace inherit the dump policy from their parent namespace, while objects not associated with any namespace are only dumped when performing a complete database dump (include_everything option).

This function is used as a fallback for object types that don't require special consideration beyond the basic namespace-based or global dumping policies. Extension membership always takes precedence over any other dumping decision.

## Parameters / Member Variables
- `*dobj`: Pointer to the DumpableObject to be evaluated for dumping
- `*fout`: Pointer to the Archive structure containing dump options and configuration
## Dependencies
- Functions called/Symbols referenced:
  - [checkExtensionMembership](../c/checkExtensionMembership.md)
  - DUMP_COMPONENT_ALL
  - DUMP_COMPONENT_NONE
  - DumpableObject (struct)
- Called from (representative examples):
  - [getPublications](../g/getPublications.md)
  - [getSubscriptions](../g/getSubscriptions.md)
  - [getSubscriptionTables](../g/getSubscriptionTables.md)
  - [getOperators](../g/getOperators.md)
  - [getCollations](../g/getCollations.md)
  - [getConversions](../g/getConversions.md)
  - [getOpclasses](../g/getOpclasses.md)
  - [getOpfamilies](../g/getOpfamilies.md)
  - [getAggregates](../g/getAggregates.md)
  - [getFuncs](../g/getFuncs.md)
  - [getEventTriggers](../g/getEventTriggers.md)
  - [getTransforms](../g/getTransforms.md)
  - [getTSParsers](../g/getTSParsers.md)
  - [getTSDictionaries](../g/getTSDictionaries.md)
  - [getTSTemplates](../g/getTSTemplates.md)
  - [getTSConfigurations](../g/getTSConfigurations.md)
  - [getForeignDataWrappers](../g/getForeignDataWrappers.md)
  - [getForeignServers](../g/getForeignServers.md)

## Notes and Other Information
- This is the default policy function for objects without specialized dumping requirements
- Objects with namespace associations inherit their dump policy from the namespace's dump_contains flag
- Objects without namespace associations are only dumped during complete dumps
- Extension membership overrides all other policy decisions
- Used by a wide variety of PostgreSQL object types including publications, subscriptions, operators, collations, functions, and text search objects