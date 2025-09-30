# DefineTSParser

## Location
[src/backend/commands/tsearchcmds.c:184-306](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tsearchcmds.c#L184-L306)

## Overview
This function implements the CREATE TEXT SEARCH PARSER SQL command, creating a new text search parser object in the system catalog with all required function references and dependencies.

## Definition
```c
ObjectAddress DefineTSParser(List *names, List *parameters)
```

## Detailed Description
The function processes a CREATE TEXT SEARCH PARSER command by validating parameters, creating a new parser entry in the pg_ts_parser system catalog, and establishing all necessary dependencies. It requires superuser privileges and validates that all required parser functions (start, gettoken, end, lextypes) are provided, while the headline function is optional.

The function extracts parser function specifications from the parameters list, validates each function using get_ts_parser_func(), creates the catalog tuple, inserts it into pg_ts_parser, and establishes dependencies through makeParserDependencies(). The process ensures that the parser is properly integrated into the dependency system and extension framework.

## Parameters / Member Variables
- `names`: List of names representing the qualified parser name (schema.parser_name)
- `parameters`: List of DefElem structures specifying parser functions (start, gettoken, end, headline, lextypes)

## Dependencies
- Functions called/Symbols referenced:
  - [superuser](../s/superuser.md): Checks if current user has superuser privileges
  - [QualifiedNameGetCreationNamespace](../Q/QualifiedNameGetCreationNamespace.md): Resolves namespace and extracts parser name
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md): Generates new OID for the parser
  - [get_ts_parser_func](../g/get_ts_parser_func.md): Validates and retrieves function OIDs for parser methods
  - [heap_form_tuple](../h/heap_form_tuple.md): Creates heap tuple from values array
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md): Inserts tuple into pg_ts_parser catalog
  - [makeParserDependencies](../m/makeParserDependencies.md): Establishes all dependency relationships
  - InvokeObjectPostCreateHook: Triggers post-creation hooks
  - [heap_freetuple](../h/heap_freetuple.md): Frees tuple memory
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md): Called during SQL command processing for CREATE TEXT SEARCH PARSER

## Notes and Other Information
- Requires superuser privileges to create text search parsers
- Four functions are mandatory: start, gettoken, end, lextypes
- The headline function is optional and can be omitted
- Parser functions are validated for correct signatures during creation
- Returns ObjectAddress of the newly created parser for dependency tracking
- Supports qualified naming (schema.parser_name) with proper namespace resolution
- Integrates with extension system and object dependency framework
- Uses row-exclusive locking on pg_ts_parser during creation

## Simplified Source

```c
ObjectAddress
DefineTSParser(List *names, List *parameters)
{
    char       *parser_name;
    ListCell   *param_cell;
    Relation    parser_rel;
    HeapTuple   tuple;
    Datum       values[Natts_pg_ts_parser];
    bool        nulls[Natts_pg_ts_parser];
    NameData    pname;
    Oid         parser_oid;
    Oid         namespace_oid;
    ObjectAddress address;

    // Require superuser privileges
    if (!superuser())
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("must be superuser to create text search parsers")));

    // Open parser catalog for insertion
    parser_rel = table_open(TSParserRelationId, RowExclusiveLock);

    // Extract namespace and parser name from qualified name
    namespace_oid = QualifiedNameGetCreationNamespace(names, &parser_name);

    // Initialize tuple data
    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));

    // Set basic parser attributes
    parser_oid = GetNewOidWithIndex(parser_rel, TSParserOidIndexId, Anum_pg_ts_parser_oid);
    values[Anum_pg_ts_parser_oid - 1] = ObjectIdGetDatum(parser_oid);
    namestrcpy(&pname, parser_name);
    values[Anum_pg_ts_parser_prsname - 1] = NameGetDatum(&pname);
    values[Anum_pg_ts_parser_prsnamespace - 1] = ObjectIdGetDatum(namespace_oid);

    // Parse and validate required parser functions
    foreach(param_cell, parameters)
    {
        DefElem *def_elem = (DefElem *) lfirst(param_cell);

        if (strcmp(def_elem->defname, "start") == 0)
        {
            values[Anum_pg_ts_parser_prsstart - 1] =
                get_ts_parser_func(def_elem, Anum_pg_ts_parser_prsstart);
        }
        else if (strcmp(def_elem->defname, "gettoken") == 0)
        {
            values[Anum_pg_ts_parser_prstoken - 1] =
                get_ts_parser_func(def_elem, Anum_pg_ts_parser_prstoken);
        }
        else if (strcmp(def_elem->defname, "end") == 0)
        {
            values[Anum_pg_ts_parser_prsend - 1] =
                get_ts_parser_func(def_elem, Anum_pg_ts_parser_prsend);
        }
        else if (strcmp(def_elem->defname, "headline") == 0)
        {
            values[Anum_pg_ts_parser_prsheadline - 1] =
                get_ts_parser_func(def_elem, Anum_pg_ts_parser_prsheadline);
        }
        else if (strcmp(def_elem->defname, "lextypes") == 0)
        {
            values[Anum_pg_ts_parser_prslextype - 1] =
                get_ts_parser_func(def_elem, Anum_pg_ts_parser_prslextype);
        }
        else
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("text search parser parameter \"%s\" not recognized",
                            def_elem->defname)));
    }

    // Validate required functions are provided
    if (!OidIsValid(DatumGetObjectId(values[Anum_pg_ts_parser_prsstart - 1])))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                 errmsg("text search parser start method is required")));

    if (!OidIsValid(DatumGetObjectId(values[Anum_pg_ts_parser_prstoken - 1])))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                 errmsg("text search parser gettoken method is required")));

    if (!OidIsValid(DatumGetObjectId(values[Anum_pg_ts_parser_prsend - 1])))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                 errmsg("text search parser end method is required")));

    if (!OidIsValid(DatumGetObjectId(values[Anum_pg_ts_parser_prslextype - 1])))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                 errmsg("text search parser lextypes method is required")));

    // Create and insert catalog tuple
    tuple = heap_form_tuple(parser_rel->rd_att, values, nulls);
    CatalogTupleInsert(parser_rel, tuple);

    // Establish dependencies and trigger hooks
    address = makeParserDependencies(tuple);
    InvokeObjectPostCreateHook(TSParserRelationId, parser_oid, 0);

    // Cleanup
    heap_freetuple(tuple);
    table_close(parser_rel, RowExclusiveLock);

    return address;
}
```