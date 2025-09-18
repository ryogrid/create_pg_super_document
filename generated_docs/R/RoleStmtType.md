# RoleStmtType

## Location
src/include/nodes/parsenodes.h: 3079 - 3080

## Overview
RoleStmtType is an enumeration that distinguishes between different types of role creation statements in PostgreSQL, specifically ROLE, USER, and GROUP statements.

## Definition


## Detailed Description
This enumeration identifies the original SQL syntax used when creating database roles in PostgreSQL. While USER and GROUP are legacy concepts that have been unified under the ROLE concept, PostgreSQL maintains backward compatibility by accepting CREATE USER and CREATE GROUP statements. The enumeration allows the system to distinguish between these different syntactic forms because they have different default behaviors.

The distinction is particularly important for CREATE statements because USER and GROUP have different default privileges and characteristics. For ALTER and DROP operations, the original syntax is less critical since the operations are functionally equivalent regardless of how the role was originally created.

## Parameters / Member Variables
- : Indicates the statement used CREATE ROLE syntax. This is the modern, preferred syntax for creating database roles with explicit privilege specifications.

- : Indicates the statement used CREATE USER syntax. This is legacy syntax that creates a role with LOGIN privilege by default, maintaining backward compatibility with older PostgreSQL versions.

- : Indicates the statement used CREATE GROUP syntax. This is legacy syntax that creates a role without LOGIN privilege by default, representing the old concept of user groups.

## Dependencies
- Functions called/Symbols referenced: None (this is an enum definition)
- Called from (representative examples):
  -  structure in src/include/nodes/parsenodes.h:3084

## Notes and Other Information
- This enum is defined in src/include/nodes/parsenodes.h:3074-3079
- The enum is used as the  field in the  structure to preserve the original SQL syntax used
- While USER and GROUP are legacy concepts, they are maintained for backward compatibility with existing applications and scripts
- The distinction affects default behaviors: USER roles default to having LOGIN privilege, while GROUP roles default to NOLOGIN
- For ALTER and DROP operations, the original syntax distinction is less important since the operations are functionally equivalent
- This enum helps the parser and command execution system apply appropriate defaults based on the original statement type
- Modern PostgreSQL applications should prefer CREATE ROLE with explicit privilege specifications rather than relying on USER/GROUP defaults