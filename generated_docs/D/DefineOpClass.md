# DefineOpClass

## Location
[src/backend/commands/opclasscmds.c:333-771](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/opclasscmds.c#L333-L771)

## Overview
Creates a new index operator class, which defines how a particular data type can be used with a specific access method by grouping together operators and support functions.

## Definition


## Detailed Description
DefineOpClass implements the CREATE OPERATOR CLASS SQL command. It creates a new operator class that defines how a specific data type can be indexed using a particular access method. The function validates all components (operators, functions, storage type), ensures proper permissions, creates the necessary catalog entries, and establishes dependency relationships.

Key responsibilities:
- Validates access method existence and retrieves its properties
- Processes and validates operators and support functions
- Handles operator family creation or lookup
- Enforces superuser privilege requirements
- Creates pg_opclass catalog entry
- Establishes dependencies between the opclass and related objects
- Calls access method-specific validation routines
- Stores operators and procedures in pg_amop and pg_amproc catalogs

## Parameters / Member Variables
- : Parsed CREATE OPERATOR CLASS statement containing:
  - : List of names forming the operator class name
  - : Access method name
  - : Data type the operator class applies to
  - : Optional operator family name
  - : List of operators, functions, and storage type specifications
  - : Whether this should be the default opclass for the data type

## Dependencies
- Functions called/Symbols referenced:
  - [QualifiedNameGetCreationNamespace](../Q/QualifiedNameGetCreationNamespace.md)
  - [object_aclcheck](../o/object_aclcheck.md)
  - [SearchSysCache1](../S/SearchSysCache1.md), SearchSysCache3
  - [GetIndexAmRoutineByAmId](../G/GetIndexAmRoutineByAmId.md)
  - superuser
  - [typenameTypeId](../t/typenameTypeId.md)
  - [get_opfamily_oid](../g/get_opfamily_oid.md)
  - [CreateOpFamily](../C/CreateOpFamily.md)
  - [LookupOperWithArgs](../L/LookupOperWithArgs.md), LookupOperName
  - [LookupFuncWithArgs](../L/LookupFuncWithArgs.md)
  - [assignOperTypes](../a/assignOperTypes.md), assignProcTypes
  - [addFamilyMember](../a/addFamilyMember.md)
  - [storeOperators](../s/storeOperators.md), storeProcedures
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - [recordDependencyOnOwner](../r/recordDependencyOnOwner.md)
  - [recordDependencyOnCurrentExtension](../r/recordDependencyOnCurrentExtension.md)
  - InvokeObjectPostCreateHook
  - [EventTriggerCollectCreateOpClass](../E/EventTriggerCollectCreateOpClass.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (utility command processing)

## Notes and Other Information
- Requires superuser privileges due to the complexity of validating operator/function consistency
- Automatically creates an operator family if none is specified and no matching family exists
- Supports three types of items: operators (OPCLASS_ITEM_OPERATOR), support functions (OPCLASS_ITEM_FUNCTION), and storage type (OPCLASS_ITEM_STORAGETYPE)
- Creates hard dependencies from pg_amop and pg_amproc entries to the operator class
- Validates that operator and function numbers are within the access method's supported ranges
- Handles both explicit operator family specification and automatic family creation/lookup
- Storage type specification is optional and validated against access method capabilities