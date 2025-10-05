# fill_ident_view

## Location
[src/backend/utils/adt/hbafuncs.c:521-573](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/hbafuncs.c#L521-L573)

## Overview
Reads the pg_ident.conf file and fills a tuplestore with view records for the pg_ident_file_mappings system view.

## Definition

```c
static void
fill_ident_view(Tuplestorestate *tuple_store, TupleDesc tupdesc)
```
## Detailed Description
This internal function is responsible for parsing PostgreSQL's pg_ident.conf authentication configuration file and populating a tuplestore with the parsed identity mapping entries. The function performs the following operations:

1. Opens the pg_ident.conf file using open_auth_file()
2. Tokenizes the entire file content into structured lines
3. Creates a temporary memory context for parsing operations
4. Iterates through each tokenized line and parses valid identity mapping entries
5. For each line (valid or invalid), creates a tuplestore entry via fill_ident_line()
6. Cleans up memory contexts and file handles

The function handles both valid configuration entries and lines with errors, ensuring that diagnostic information is preserved in the resulting view. Each successfully parsed mapping is assigned an incrementing map_number for identification.

## Parameters / Member Variables
- `*tuple_store`: Tuplestorestate pointer where the parsed identity mapping records will be stored
- `tupdesc`: TupleDesc describing the structure of the target view's tuples
## Dependencies
- Functions called/Symbols referenced:
  - [open_auth_file](../o/open_auth_file.md)
  - [tokenize_auth_file](../t/tokenize_auth_file.md)  
  - AllocSetContextCreate
  - [parse_ident_line](../p/parse_ident_line.md)
  - [fill_ident_line](fill_ident_line.md)
  - [free_auth_file](free_auth_file.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
- Called from (representative examples):
  - [pg_ident_file_mappings](../p/pg_ident_file_mappings.md)

## Notes and Other Information
- This is a static (internal) function used exclusively by the pg_ident_file_mappings SQL function
- Uses a dedicated memory context ('ident parser context') to manage memory for parsing operations
- Handles file access errors by throwing exceptions rather than returning error entries in the view
- Maintains sequential map numbering only for successfully parsed entries (lines with errors don't increment the counter)
- Part of PostgreSQL's Host-Based Authentication (HBA) system for identity mapping between system and database users
- Located in src/backend/utils/adt/hbafuncs.c:521-573

## Simplified Source

```c
static void
fill_ident_view(Tuplestorestate *tuple_store, TupleDesc tupdesc)
{
    FILE *file;
    List *ident_lines = NIL;
    ListCell *line;
    int map_number = 0;
    MemoryContext identcxt, oldcxt;

    // Open and tokenize identity mapping configuration file
    file = open_auth_file(IdentFileName, ERROR, 0, NULL);
    tokenize_auth_file(IdentFileName, file, &ident_lines, DEBUG3, 0);

    // Create temporary memory context for parsing
    identcxt = AllocSetContextCreate(CurrentMemoryContext,
                                     "ident parser context",
                                     ALLOCSET_SMALL_SIZES);
    oldcxt = MemoryContextSwitchTo(identcxt);

    // Process each line from the identity file
    foreach(line, ident_lines)
    {
        TokenizedAuthLine *tok_line = (TokenizedAuthLine *) lfirst(line);
        IdentLine *identline = NULL;

        // Parse valid lines only
        if (tok_line->err_msg == NULL) {
            identline = parse_ident_line(tok_line, DEBUG3);
            map_number++;  // Increment only for valid mappings
        }

        // Add line to tuplestore (both valid and invalid)
        fill_ident_line(tuple_store, tupdesc, map_number,
                        tok_line->file_name, tok_line->line_num,
                        identline, tok_line->err_msg);
    }

    // Cleanup resources
    free_auth_file(file, 0);
    MemoryContextSwitchTo(oldcxt);
    MemoryContextDelete(identcxt);
}
```