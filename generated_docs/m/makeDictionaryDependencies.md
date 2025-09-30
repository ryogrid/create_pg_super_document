# makeDictionaryDependencies

## Location
[src/backend/commands/tsearchcmds.c:307-341](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tsearchcmds.c#L307-L341)

## Overview
This function creates pg_depend entries for a new text search dictionary, establishing dependencies on owner, namespace, template, and extension to ensure proper cascading behavior.

## Definition
```c
static ObjectAddress makeDictionaryDependencies(HeapTuple tuple)
```

## Detailed Description
The function establishes a complete dependency graph for a newly created text search dictionary by recording dependencies in the pg_depend system catalog. It extracts dictionary information from the provided HeapTuple and creates dependency records for the dictionary's owner (user/role), namespace, and template. Additionally, it records the dictionary's membership in the current extension if executed within an extension context.

Unlike parser dependencies, dictionary dependencies include an ownership dependency which is handled separately through recordDependencyOnOwner(). The namespace and template dependencies use normal dependency strength, ensuring the dictionary will be automatically dropped if its namespace or template are dropped.

## Parameters / Member Variables
- `tuple`: HeapTuple containing the pg_ts_dict row data for the new dictionary

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_ts_dict: Type cast to access dictionary tuple fields
  - ObjectAddressSet: Sets up object address structures
  - [recordDependencyOnOwner](../r/recordDependencyOnOwner.md): Records ownership dependency between dictionary and owner
  - [recordDependencyOnCurrentExtension](../r/recordDependencyOnCurrentExtension.md): Records extension membership
  - [new_object_addresses](../n/new_object_addresses.md): Creates new ObjectAddresses collection
  - [add_exact_object_address](../a/add_exact_object_address.md): Adds object to dependency collection
  - [record_object_address_dependencies](../r/record_object_address_dependencies.md): Records all dependencies with specified strength
  - [free_object_addresses](../f/free_object_addresses.md): Cleans up ObjectAddresses collection
- Called from (representative examples):
  - [DefineTSDictionary](../D/DefineTSDictionary.md): Called after inserting new dictionary tuple to establish dependencies

## Notes and Other Information
- This is a static function, only accessible within tsearchcmds.c
- Records four types of dependencies: owner, extension, namespace, and template
- Owner dependency is handled separately from namespace/template dependencies
- Uses DEPENDENCY_NORMAL strength for namespace and template dependencies
- The function returns the ObjectAddress of the dictionary itself for potential use by callers
- Template dependency ensures dictionary is dropped if its template is removed
- Ownership dependency enables proper permission and ownership tracking

## Simplified Source

```c
static ObjectAddress makeDictionaryDependencies(HeapTuple tuple) {
    Form_pg_ts_dict dict = (Form_pg_ts_dict) GETSTRUCT(tuple);
    ObjectAddress myself, referenced;
    ObjectAddresses *addrs;

    // Set up dictionary object address
    ObjectAddressSet(myself, TSDictionaryRelationId, dict->oid);

    // Record ownership dependency
    recordDependencyOnOwner(myself.classId, myself.objectId, dict->dictowner);

    // Record extension dependency if in extension context
    recordDependencyOnCurrentExtension(&myself, false);

    addrs = new_object_addresses();

    // Add namespace dependency
    ObjectAddressSet(referenced, NamespaceRelationId, dict->dictnamespace);
    add_exact_object_address(&referenced, addrs);

    // Add template dependency
    ObjectAddressSet(referenced, TSTemplateRelationId, dict->dicttemplate);
    add_exact_object_address(&referenced, addrs);

    // Record all collected dependencies
    record_object_address_dependencies(&myself, addrs, DEPENDENCY_NORMAL);
    free_object_addresses(addrs);

    return myself;
}
```