# AlterObjectSchemaStmt

## Location
src/include/nodes/parsenodes.h: 3557 - 3565

## Overview
AlterObjectSchemaStmt is a PostgreSQL parse node structure that represents an ALTER object SET SCHEMA statement for moving database objects between schemas.

## Definition


## Detailed Description
AlterObjectSchemaStmt represents SQL statements that move database objects from one schema to another. This is commonly used for reorganizing database objects, moving objects to different namespaces, or implementing schema-based access control. The structure can handle various object types including tables, types, functions, and other schema-scoped objects. It includes error handling through the missing_ok flag for IF EXISTS semantics.

## Parameters / Member Variables
- : Standard NodeTag for parse tree identification
- : Specifies the type of object being moved (OBJECT_TABLE, OBJECT_TYPE, etc.)
- : RangeVar pointer used when the object being moved is a table or relation
- : Generic Node pointer for other types of objects being moved
- : String containing the name of the destination schema
- : Boolean flag to suppress errors if the source object doesn't exist (IF EXISTS semantics)

## Dependencies
- Functions called/Symbols referenced:
  - ObjectType (enumeration for database object types)
  - RangeVar (structure for table/relation references)
  - NodeTag (standard parse node identification)
- Called from (representative examples):
  - ExecAlterObjectSchemaStmt (main execution function)
  - AlterTableNamespace (table-specific schema change)
  - standard_ProcessUtility (utility command processing)
  - RangeVarCallbackForAlterRelation (relation-specific callback)

## Notes and Other Information
This statement type is essential for database organization and namespace management in PostgreSQL. Moving objects between schemas can affect access permissions, search paths, and object resolution. The operation includes dependency checking to ensure that moving an object doesn't break references from other database objects. The missing_ok flag provides graceful handling of non-existent objects in automated scripts.