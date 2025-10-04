# typeStringToTypeName

## Location
[src/backend/parser/parse_type.c:738-784](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_type.c#L738-L784)

## Overview
Parses a SQL-compatible type declaration string and returns a TypeName node representing the parsed type information.

## Definition
```c
TypeName *typeStringToTypeName(const char *str, Node *escontext)
```

## Detailed Description
This function takes a string representation of a SQL type (such as "int4", "integer", or "character varying(32)") and parses it using PostgreSQL's raw parser to create a TypeName node. The function handles the complete parsing process including error handling and validation.

The function sets up an error context callback to provide meaningful error messages if parsing fails. It uses the raw parser with RAW_PARSE_TYPE_NAME mode to parse the type string according to SQL grammar rules.

Key validation steps include:
- Checking for empty or whitespace-only input
- Ensuring exactly one TypeName node is returned from parsing
- Rejecting SETOF types (not allowed in this context)

The function supports soft error handling through the escontext parameter, allowing callers to handle errors gracefully rather than having them thrown immediately.

## Parameters / Member Variables
- `str`: The string containing the SQL type declaration to parse
- `escontext`: Error context node for soft error handling; if NULL, errors are thrown normally

## Dependencies
- Functions called/Symbols referenced:
  - [pts_error_callback](../p/pts_error_callback.md)
  - [raw_parser](../r/raw_parser.md)
  - RAW_PARSE_TYPE_NAME
  - linitial_node
  - unconstify
  - ereturn
  - strspn
  - strlen
- Called from (representative examples):
  - [pg_get_object_address](../p/pg_get_object_address.md) (src/backend/catalog/objectaddress.c:2147, 2203)
  - [parseTypeString](../p/parseTypeString.md) (src/backend/parser/parse_type.c:791)

## Notes and Other Information
- Returns NULL on parse failure when escontext is provided for soft error handling
- Throws ERROR on parse failure when escontext is NULL
- Rejects SETOF type constructs even if they parse successfully
- Uses PostgreSQL's standard error context callback mechanism for better error reporting
- The ErrorSaveContext option is noted as "mostly aspirational" - some grammar errors may still be thrown
- Input string is checked for being empty or containing only whitespace characters

## Simplified Source

```c
TypeName *
typeStringToTypeName(const char *str, Node *escontext)
{
    List *raw_parsetree_list;
    TypeName *typeName;
    ErrorContextCallback ptserrcontext;

    // Check for empty or whitespace-only input
    if (strspn(str, " \t\n\r\f\v") == strlen(str))
        goto fail;

    // Setup error context for better error messages
    ptserrcontext.callback = pts_error_callback;
    ptserrcontext.arg = unconstify(char *, str);
    ptserrcontext.previous = error_context_stack;
    error_context_stack = &ptserrcontext;

    // Parse the type string using raw parser
    raw_parsetree_list = raw_parser(str, RAW_PARSE_TYPE_NAME);

    // Restore previous error context
    error_context_stack = ptserrcontext.previous;

    // Should get exactly one TypeName node
    Assert(list_length(raw_parsetree_list) == 1);
    typeName = linitial_node(TypeName, raw_parsetree_list);

    // Reject SETOF types (not allowed in this context)
    if (typeName->setof)
        goto fail;

    return typeName;

fail:
    ereturn(escontext, NULL,
            (errcode(ERRCODE_SYNTAX_ERROR),
             errmsg("invalid type name \"%s\"", str)));
}
```