# _PQprintOpt

## Location
[src/interfaces/libpq/libpq-fe.h:231-243](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/libpq-fe.h#L231-L243)

## Overview
The  struct defines formatting options for printing PostgreSQL query results. It provides control over various output formats including plain text, HTML tables, and field formatting options.

## Definition


## Detailed Description
The  structure is a configuration container for controlling the output formatting of PostgreSQL query results through libpq. It supports multiple output formats including standard text, aligned columns, HTML tables, and expanded (vertical) display modes. The structure allows fine-grained control over field separators, HTML table attributes, captions, and custom field names. This struct is typically used with PostgreSQL client applications that need to format query results for display or export purposes.

## Parameters / Member Variables
- : Controls whether to print column headings and row count information
- : Enables field alignment for tabular output formatting
- : Uses the legacy "brain dead" format for backward compatibility
- : Enables HTML table output format
- : Switches to expanded (vertical) table display mode
- : Automatically uses a pager for output when needed
- : String used as field separator between columns
- : HTML attributes to insert into the <table> tag
- : HTML caption text for table output
- : Array of custom field names to replace default column names

## Dependencies
- Functions called/Symbols referenced:
  - pqbool (boolean type used for flags)
- Called from (representative examples):
  - (No direct references found in the indexed codebase)

## Notes and Other Information
- This struct is defined in the libpq public header file (libpq-fe.h), making it part of the client-facing API
- The structure supports both text-based and HTML output formats
- Field names can be customized through the fieldName array, which must be null-terminated
- The pager option allows automatic pagination for large result sets
- The structure is aliased as  for public use