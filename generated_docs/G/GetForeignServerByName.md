# GetForeignServerByName

## Location
src/backend/foreign/foreign.c: 182 - 199

## Overview
Looks up a foreign server definition by name and returns the corresponding ForeignServer structure.

## Definition


## Detailed Description
GetForeignServerByName is a utility function that retrieves a foreign server object by its name. It serves as a wrapper around the lower-level functions, first converting the server name to an OID using get_foreign_server_oid(), then retrieving the full ForeignServer structure using GetForeignServer(). The function provides error handling based on the missing_ok parameter - if the server doesn't exist and missing_ok is false, an error will be raised; if missing_ok is true, NULL is returned instead.

## Parameters / Member Variables
- `srvname`: The name of the foreign server to look up
- `missing_ok`: Boolean flag indicating whether to raise an error (false) or return NULL (true) when the server is not found

## Dependencies
- Functions called/Symbols referenced:
  - [get_foreign_server_oid](../g/get_foreign_server_oid.md)
  - [GetForeignServer](GetForeignServer.md)
  - OidIsValid (macro)
- Called from (representative examples):
  - [CreateUserMapping](../C/CreateUserMapping.md)
  - [AlterUserMapping](../A/AlterUserMapping.md)  
  - [RemoveUserMapping](../R/RemoveUserMapping.md)
  - [CreateForeignTable](../C/CreateForeignTable.md)
  - [ImportForeignSchema](../I/ImportForeignSchema.md)

## Notes and Other Information
This function is commonly used in foreign data wrapper (FDW) operations where server names need to be resolved to their corresponding server objects. The two-step lookup process (name → OID → ForeignServer) follows PostgreSQL's typical pattern for object resolution. The function is located in src/backend/foreign/foreign.c:182-199.