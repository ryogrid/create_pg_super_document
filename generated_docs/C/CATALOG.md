# CATALOG

## Location
src/include/catalog/pg_shdescription.h: 41 - 55

## Overview
The CATALOG macro is a fundamental preprocessor macro that introduces the structure definition for PostgreSQL system catalog tables, serving as the foundation for defining all system catalog schemas in a way that can be processed by both the C compiler and the BKI (Bootstrap Interface) generation scripts.

## Definition
```c
#define CATALOG(name,oid,oidmacro)	typedef struct CppConcat(FormData_,name)
```

## Detailed Description
The CATALOG macro is defined in `src/include/catalog/genbki.h:23` and serves as the primary mechanism for defining PostgreSQL system catalog table structures. When invoked, it creates a C typedef that defines a structure representing the catalog table format. The macro expands to create a structure name by concatenating "FormData_" with the provided catalog name.

This macro is designed to work in conjunction with the genbki.pl script, which processes catalog header files to generate the BKI (Bootstrap Interface) files used during PostgreSQL initialization. The macro allows the same catalog definitions to be understood by both the C compiler (for backend code compilation) and the bootstrap generation tools.

The CATALOG macro typically appears at the beginning of system catalog header files and is followed by a structure definition containing the catalog's columns. Each catalog definition specifies three key pieces of information: the catalog name, its OID (Object Identifier), and a macro name for the OID.

## Parameters / Member Variables
- `name`: The name of the catalog table (e.g., pg_class, pg_type, pg_proc)
- `oid`: The numeric Object Identifier assigned to this catalog table 
- `oidmacro`: The C macro name that will be defined to represent this catalog's OID (e.g., RelationRelationId)

## Dependencies
- Functions called/Symbols referenced:
  - CppConcat (macro for token concatenation)
- Called from (representative examples):
  - Used extensively throughout catalog header files in src/include/catalog/
  - pg_class.h defines `CATALOG(pg_class,1259,RelationRelationId)`
  - pg_type.h defines `CATALOG(pg_type,1247,TypeRelationId)`
  - pg_proc.h defines `CATALOG(pg_proc,1255,ProcedureRelationId)`
  - All other system catalog definitions (60+ catalog tables)

## Notes and Other Information
- The CATALOG macro is often followed by additional BKI options on the same line, such as BKI_BOOTSTRAP, BKI_SHARED_RELATION, BKI_ROWTYPE_OID, and BKI_SCHEMA_MACRO
- The generated structure name follows the pattern FormData_[catalogname], which is used throughout PostgreSQL backend code to represent catalog tuple data
- This macro is essential for the bootstrap process, as genbki.pl recognizes it to generate the initial database catalog structure
- The macro is defined with empty implementations of various BKI directives to prevent C compiler errors while allowing the genbki.pl script to parse the special directives
- Catalogs marked with BKI_BOOTSTRAP are created during the initial bootstrap phase, while others are created later in the initialization process
- The macro system enables a single source of truth for catalog definitions that serves both runtime code generation and bootstrap data generation needs