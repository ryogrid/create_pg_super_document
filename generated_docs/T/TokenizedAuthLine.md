# TokenizedAuthLine

## Location
[src/include/libpq/hba.h:158-165](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/libpq/hba.h#L158-L165)

## Overview
TokenizedAuthLine represents the lexically parsed form of a single line from an authentication configuration file, containing tokenized fields and metadata about the parsing process.

## Definition

```c
typedef struct TokenizedAuthLine
{
	List	   *fields;			/* List of lists of AuthTokens */
	char	   *file_name;		/* File name of origin */
	int			line_num;		/* Line number */
	char	   *raw_line;		/* Raw line text */
	char	   *err_msg;		/* Error message if any */
} TokenizedAuthLine;
```
## Detailed Description
TokenizedAuthLine is an intermediate representation created during the parsing of PostgreSQL authentication configuration files (pg_hba.conf and pg_ident.conf). It represents a single line that has been lexically analyzed and broken down into structured token fields. Each line is tokenized into a list of field groups, where each field group contains one or more AuthToken structures. This structure serves as the bridge between raw configuration text and the final parsed authentication rules (HbaLine or IdentLine structures). The structure includes comprehensive metadata for error reporting and debugging, including the source file name, line number, original text, and any error messages that occurred during tokenization.

## Parameters / Member Variables
- : List of lists of AuthTokens representing the structured token groups for this line. Each sub-list contains tokens that belong to the same logical field
- : Name of the configuration file where this line originated (pg_hba.conf or pg_ident.conf)
- : Line number within the source file for error reporting and debugging purposes
- : Original unparsed line text as it appeared in the configuration file
- : Error message string if tokenization failed, NULL if parsing was successful

## Dependencies
- Functions called/Symbols referenced:
  - [List](../L/List.md) (PostgreSQL list data structure)
  - [AuthToken](../A/AuthToken.md) (indirectly through fields structure)
- Called from (representative examples):
  - [tokenize_expand_file](../t/tokenize_expand_file.md)
  - [tokenize_auth_file](../t/tokenize_auth_file.md)
  - [parse_hba_line](../p/parse_hba_line.md)
  - [load_hba](../l/load_hba.md)
  - [parse_ident_line](../p/parse_ident_line.md)
  - [load_ident](../l/load_ident.md)
  - [fill_hba_view](../f/fill_hba_view.md)
  - [fill_ident_view](../f/fill_ident_view.md)

## Notes and Other Information
- Serves as an intermediate parsing stage between raw configuration text and final authentication structures
- Empty lines and comment-only lines are not represented by TokenizedAuthLine structures
- The fields member is never NULL for successfully tokenized lines, and none of its sub-lists are empty
- If tokenization fails, fields may be NULL and err_msg will contain the error description
- Essential component of PostgreSQL's configuration parsing pipeline
- Enables detailed error reporting with precise file and line number information
- Memory management handled by the authentication file parsing subsystem
- The nested list structure (list of lists) allows for complex field grouping during tokenization