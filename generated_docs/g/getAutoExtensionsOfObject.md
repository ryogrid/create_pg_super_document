# getAutoExtensionsOfObject

## Location
src/backend/catalog/pg_depend.c: 779 - 828

## Overview
Returns a list of extensions that the specified object depends on through DEPENDENCY_AUTO_EXTENSION relationships.

## Definition


## Detailed Description
The `getAutoExtensionsOfObject` function searches the `pg_depend` system catalog to find all extensions that have an automatic extension dependency relationship with a given database object. Unlike regular extension membership (DEPENDENCY_EXTENSION), automatic extension dependencies (DEPENDENCY_AUTO_EXTENSION) represent a different kind of relationship where objects are automatically associated with extensions but can exist independently.

The function performs a comprehensive scan of the `pg_depend` table, collecting all dependency records where:
- The `classid` and `objid` match the specified object
- The `refclassid` points to ExtensionRelationId (indicating the dependency target is an extension)
- The `deptype` is DEPENDENCY_AUTO_EXTENSION (indicating automatic extension dependency)

For each matching dependency, the function adds the extension's OID (`refobjid`) to the result list. The function continues scanning through all dependency records to collect all applicable extensions, as objects may have automatic dependencies on multiple extensions.

## Parameters / Member Variables
- `classId`: The OID of the system catalog that contains the object (e.g., RelationRelationId for tables)
- `objectId`: The OID of the specific object within that catalog

## Dependencies
- Functions called/Symbols referenced:
  - [SysScanDesc](../S/SysScanDesc.md)
  - [systable_beginscan](../s/systable_beginscan.md)  
  - [systable_getnext](../s/systable_getnext.md)
  - Form_pg_depend
  - DEPENDENCY_AUTO_EXTENSION
  - lappend_oid
- Called from (representative examples):
  - [ExecAlterObjectDependsStmt](../E/ExecAlterObjectDependsStmt.md)
  - PERFORM_DELETION_CONCURRENT_LOCK

## Notes and Other Information
- Returns NIL (empty list) if the object has no automatic extension dependencies
- Unlike `getExtensionOfObject`, this function can return multiple extensions since objects may have automatic dependencies on several extensions
- Uses the `lappend_oid` function to build the result list dynamically during the scan
- The DEPENDENCY_AUTO_EXTENSION dependency type represents a weaker form of extension association compared to regular extension membership
- This function is particularly important for the ALTER ... DEPENDS ON EXTENSION functionality
- Uses AccessShareLock when accessing the pg_depend catalog for consistent reads