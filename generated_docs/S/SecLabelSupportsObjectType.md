# SecLabelSupportsObjectType

## Location
src/backend/commands/seclabel.c: 37 - 114

## Overview
Determines whether security labels are supported for a given PostgreSQL object type by checking the object type against a predefined list of supported types.

## Definition


## Detailed Description
This function serves as a filter to validate whether security labels can be applied to specific PostgreSQL database objects. It implements a whitelist approach, explicitly returning  for object types that support security labeling and  for those that do not. The function uses a comprehensive switch statement to categorize all known PostgreSQL object types into supported and unsupported groups.

The supported object types include core database objects like tables, views, functions, schemas, roles, and other major database entities. Unsupported types typically include internal system objects, constraints, operators, and auxiliary objects that don't require or benefit from security labeling.

## Parameters / Member Variables
- : An  enum value representing the type of PostgreSQL object to check for security label support

## Dependencies
- Functions called/Symbols referenced:
  - ObjectType (enum type)
  - Various OBJECT_* constants (OBJECT_TABLE, OBJECT_VIEW, etc.)
- Called from (representative examples):
  - ExecSecLabelStmt

## Notes and Other Information
- The function is intentionally implemented without a default case in the switch statement to ensure compiler warnings if new ObjectType values are added without being explicitly handled
- Supported object types include: AGGREGATE, COLUMN, DATABASE, DOMAIN, EVENT_TRIGGER, FOREIGN_TABLE, FUNCTION, LANGUAGE, LARGEOBJECT, MATVIEW, PROCEDURE, PUBLICATION, ROLE, ROUTINE, SCHEMA, SEQUENCE, SUBSCRIPTION, TABLE, TABLESPACE, TYPE, VIEW
- Unsupported object types include: ACCESS_METHOD, AMOP, AMPROC, ATTRIBUTE, CAST, COLLATION, CONVERSION, DEFAULT, DEFACL, DOMCONSTRAINT, EXTENSION, FDW, FOREIGN_SERVER, INDEX, OPCLASS, OPERATOR, OPFAMILY, PARAMETER_ACL, POLICY, PUBLICATION_NAMESPACE, PUBLICATION_REL, RULE, STATISTIC_EXT, TABCONSTRAINT, TRANSFORM, TRIGGER, TSCONFIGURATION, TSDICTIONARY, TSPARSER, TSTEMPLATE, USER_MAPPING
- This function acts as a central validation point for security label operations, ensuring that security labels are only applied to appropriate object types