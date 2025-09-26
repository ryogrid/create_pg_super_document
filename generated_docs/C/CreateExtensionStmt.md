# CreateExtensionStmt

## Location
src/include/nodes/parsenodes.h: 2819 - 2825

## Overview
CreateExtensionStmt represents the parsed structure for a CREATE EXTENSION statement, used to install PostgreSQL extensions that provide additional functionality to the database.

## Definition


## Detailed Description
This structure represents the CREATE EXTENSION SQL command, which is used to install extensions that add new functionality to PostgreSQL. Extensions can include new data types, functions, operators, index methods, and more. The statement supports various options like specifying the target schema, version, and cascade behavior for dependencies.

Extensions are installed from control files and SQL scripts located in the PostgreSQL installation's extension directory. The system prevents nested extension creation and ensures extension names are valid and unique within the database.

## Parameters / Member Variables
- : NodeTag identifier indicating this is a CreateExtensionStmt node
- : Name of the extension to be created/installed
- : Boolean flag controlling behavior when extension already exists (true = issue NOTICE and continue, false = raise ERROR)
- : List of DefElem structures containing optional parameters like schema, version, and cascade settings

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (from node system)
  - List (from PostgreSQL's list implementation)
  - DefElem (for option specification)
- Called from (representative examples):
  - CreateExtension (main execution function)
  - ProcessUtilitySlow (utility command processor)

## Notes and Other Information
- Common options include:
  - 'schema': Target schema for the extension objects
  - 'version': Specific version to install
  - 'cascade': Whether to automatically install dependent extensions
- Extension names must be valid identifiers and the extension files must exist in the system
- Only one extension can be created at a time (nested CREATE EXTENSION is not supported)
- The operation registers the extension in the pg_extension system catalog
- Extensions can be relocated between schemas after installation using ALTER EXTENSION
- Requires appropriate privileges to install extensions in the target database