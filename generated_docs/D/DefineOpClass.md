# DefineOpClass

## Location
src/backend/commands/opclasscmds.c: 333 - 771

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
  - QualifiedNameGetCreationNamespace
  - object_aclcheck
  - SearchSysCache1, SearchSysCache3
  - GetIndexAmRoutineByAmId
  - superuser
  - typenameTypeId
  - get_opfamily_oid
  - CreateOpFamily
  - LookupOperWithArgs, LookupOperName
  - LookupFuncWithArgs
  - assignOperTypes, assignProcTypes
  - addFamilyMember
  - storeOperators, storeProcedures
  - recordDependencyOn
  - recordDependencyOnOwner
  - recordDependencyOnCurrentExtension
  - InvokeObjectPostCreateHook
  - EventTriggerCollectCreateOpClass
- Called from (representative examples):
  - ProcessUtilitySlow (utility command processing)

## Notes and Other Information
- Requires superuser privileges due to the complexity of validating operator/function consistency
- Automatically creates an operator family if none is specified and no matching family exists
- Supports three types of items: operators (OPCLASS_ITEM_OPERATOR), support functions (OPCLASS_ITEM_FUNCTION), and storage type (OPCLASS_ITEM_STORAGETYPE)
- Creates hard dependencies from pg_amop and pg_amproc entries to the operator class
- Validates that operator and function numbers are within the access method's supported ranges
- Handles both explicit operator family specification and automatic family creation/lookup
- Storage type specification is optional and validated against access method capabilities