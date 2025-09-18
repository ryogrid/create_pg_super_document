# listTSParsers

## Location
[src/bin/psql/describe.c:5147-5198](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L5147-L5198)

## Overview
The  function implements the  psql command for displaying text search parser information in a PostgreSQL database.

## Definition


## Detailed Description
This function queries the  system catalog to retrieve and display information about text search parsers defined in the database. Text search parsers are components of PostgreSQL's full-text search functionality that break documents into tokens for indexing and searching. Each parser defines how to recognize and classify different types of text elements.

The function provides a simple listing mode that shows schema, name, and description of parsers. When verbose mode is requested, it delegates to  which provides more detailed information about parser functions and token types.

The query joins with the namespace catalog to show schema information and uses the visibility function to respect search path settings.

## Parameters / Member Variables
- : A SQL name pattern (with optional wildcards) to filter which text search parsers to display. If NULL, all visible parsers are shown.
- : If true, delegates to  for detailed parser information; if false, shows basic parser listing with schema, name, and description.

## Dependencies
- Functions called/Symbols referenced:
  - : Called when verbose output is requested for detailed parser information
  - : Initializes a dynamic string buffer
  - : Adds formatted text to the buffer
  - : Validates and processes SQL name patterns with wildcards
  - : Executes the constructed SQL query
  - : Formats and displays the query results
  - : Cleans up the string buffer
  - : Frees the query result
- Called from (representative examples):
  - : Main dispatcher for psql describe commands

## Notes and Other Information
- The function returns  on success,  on failure
- Uses error handling with cleanup on validation failures
- The basic mode shows three columns: Schema, Name, and Description
- Parser visibility is determined by  function
- Results are ordered by schema name and parser name
- Text search parsers are essential components for full-text search functionality
- Common built-in parsers include 'default' parser for general text processing
- Custom parsers can be created for specialized document types or languages
- The function serves as a gateway to either simple or verbose parser information depending on the verbose parameter