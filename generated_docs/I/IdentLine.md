# IdentLine

## Location
src/include/libpq/hba.h: 140 - 147

## Overview
IdentLine is a structure that represents a single parsed line from the pg_ident.conf configuration file, defining user mapping rules between system users and PostgreSQL users.

## Definition


## Detailed Description
IdentLine represents a parsed identity mapping rule from PostgreSQL's user identity mapping configuration file (pg_ident.conf). This structure defines how system user identities (such as operating system usernames or external authentication system identities) are mapped to PostgreSQL database user names. Each line specifies a mapping name, a pattern for the system username, and the corresponding PostgreSQL username. The AuthToken pointers allow for both literal usernames and regular expression patterns, providing flexible user mapping capabilities for authentication methods like ident and peer.

## Parameters / Member Variables
- : Line number within the pg_ident.conf file for error reporting and debugging
- : Name identifier for this user mapping rule, referenced by HBA lines that use ident or peer authentication
- : AuthToken containing the system username pattern (can be literal or regex)
- : AuthToken containing the PostgreSQL username pattern (can be literal or regex)

## Dependencies
- Functions called/Symbols referenced:
  - [AuthToken](../A/AuthToken.md) (for system_user and pg_user patterns)
- Called from (representative examples):
  - [load_hba](../l/load_hba.md)
  - [parse_ident_line](../p/parse_ident_line.md)
  - [check_ident_usermap](../c/check_ident_usermap.md)
  - [load_ident](../l/load_ident.md)
  - [fill_ident_line](../f/fill_ident_line.md)
  - [fill_ident_view](../f/fill_ident_view.md)

## Notes and Other Information
- Used in conjunction with HBA lines that specify ident or peer authentication methods
- The usermap field must match the usermap specified in corresponding pg_hba.conf entries
- Both system_user and pg_user can contain regular expressions when the AuthToken string begins with '/'
- Essential for secure user identity mapping in PostgreSQL's authentication system
- Part of PostgreSQL's external authentication infrastructure alongside HBA configuration
- Memory management handled by functions in src/backend/libpq/hba.c
- The structure supports the pg_ident.conf file format: mapname systemuser pguser