# AssignTypeArrayOid

## Location
src/backend/commands/typecmds.c: 2410 - 2442

## Overview
AssignTypeArrayOid is a function that pre-assigns an OID for the array type associated with a PostgreSQL type, ensuring proper setup of the pg_type.typarray field during type creation.

## Definition


## Detailed Description
This function is responsible for allocating a unique OID that will be used for the array type corresponding to a base type being created. The function handles two distinct scenarios:

1. **Binary Upgrade Mode**: When PostgreSQL is in binary upgrade mode (during pg_upgrade operations), it uses a pre-determined OID stored in  to maintain consistency with the original database schema.

2. **Normal Operation**: During regular type creation, it generates a new unique OID by calling  on the pg_type system catalog.

The function ensures that every type has a properly assigned array type OID before the type definition is completed, which is essential for PostgreSQL's type system where every base type can have an associated array type.

## Parameters / Member Variables
This function takes no parameters and returns:
- **Return value**:  - A unique object identifier for the array type

## Dependencies
- Functions called/Symbols referenced:
  -  (macro/variable check)
  -  (macro for OID validation)
  -  (error reporting function)
  -  (system catalog access)
  -  (OID generation function)
  -  (system catalog cleanup)
  
- Called from (representative examples):
  -  (src/backend/catalog/heap.c:1334)
  -  (src/backend/commands/typecmds.c:563)
  -  (src/backend/commands/typecmds.c:1018)
  -  (src/backend/commands/typecmds.c:1183)
  -  (src/backend/commands/typecmds.c:1523)

## Notes and Other Information
- This function is critical for maintaining referential integrity in PostgreSQL's type system
- During binary upgrades, the function validates that the required OID has been properly set, throwing an error if not
- The function uses AccessShareLock when accessing the pg_type catalog to prevent conflicts
- After using the binary upgrade OID, it resets  to InvalidOid to prevent reuse
- The allocated OID is used later in the type creation process to establish the relationship between a type and its array type