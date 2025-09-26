# SecLabelStmt

## Location
src/include/nodes/parsenodes.h: 3264 - 3271

## Overview
SecLabelStmt represents the parsed form of SQL SECURITY LABEL statements, which are used to assign or modify security labels on database objects for mandatory access control systems.

## Definition
```c
typedef struct SecLabelStmt
{
    NodeTag     type;
    ObjectType  objtype;        /* Object's type */
    Node       *object;         /* Qualified name of the object */
    char       *provider;       /* Label provider (or NULL) */
    char       *label;          /* New security label to be assigned */
} SecLabelStmt;
```

## Detailed Description
The SecLabelStmt structure is a parse tree node that encapsulates all information needed to execute a SECURITY LABEL SQL statement. Security labels are used by label-based mandatory access control (MAC) systems like SELinux to enforce security policies on database objects. The statement can specify a particular security label provider or use the default provider. When executed, this statement applies the specified security label to the target database object, which can be tables, columns, functions, schemas, and other database entities.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a SecLabelStmt parse node
- `objtype`: ObjectType enum value specifying what kind of database object is being labeled
- `object`: Node pointer to the qualified name representation of the target object
- `provider`: Character string naming the security label provider, or NULL to use the default provider
- `label`: Character string containing the security label to be assigned to the object

## Dependencies
- Functions called/Symbols referenced:
  - ObjectType (enum for database object types)
  - Node (base parse tree node type)
  - NodeTag (parse node type identifier)
- Called from (representative examples):
  - ExecSecLabelStmt (seclabel.c:115)
  - standard_ProcessUtility (utility.c:1054)
  - ProcessUtilitySlow (utility.c:1835)

## Notes and Other Information
SecLabelStmt is processed by the utility command execution system and handled by ExecSecLabelStmt() function. Security label support must be enabled at compile time and requires appropriate security label providers to be loaded. The most common provider is SELinux, but the architecture supports pluggable providers. Security label operations are transactional and integrate with PostgreSQL's privilege system.