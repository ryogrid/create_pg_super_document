# getTokenTypes

## Location
[src/backend/commands/tsearchcmds.c:1229-1287](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tsearchcmds.c#L1229-L1287)

## Overview
A static function that translates a list of token type names into a list of unique TSTokenTypeItem structures, validating token names against a text search parser's lexical types.

## Definition

```c
static List *
getTokenTypes(Oid prsId, List *tokennames)
```
## Detailed Description
This function converts string-based token type names into structured TSTokenTypeItem objects by looking up their corresponding lexical identifiers from a text search parser. It first retrieves the parser's cache entry and calls the parser's lextype method to get the available token types. For each input token name, it searches through the parser's lexical descriptor list to find matching aliases. The function automatically removes duplicates by using tstoken_list_member to check if a token is already in the result list. If a token name is not found in the parser's lexical types, it raises an error with ERRCODE_INVALID_PARAMETER_VALUE.

## Parameters / Member Variables
- `prsId`: Object ID of the text search parser to use for token type validation
- `*tokennames`: List of String nodes containing token type names to be converted
## Dependencies
- Functions called/Symbols referenced:
  - [lookup_ts_parser_cache](../l/lookup_ts_parser_cache.md)
  - [TSParserCacheEntry](../T/TSParserCacheEntry.md)
  - LexDescr
  - OidFunctionCall1
  - [String](../S/String.md)
  - [tstoken_list_member](../t/tstoken_list_member.md)
  - TSTokenTypeItem
  - [list_length](../l/list_length.md), lfirst_node, lappend, palloc0, pstrdup, strVal
- Called from (representative examples):
  - [MakeConfigurationMapping](../M/MakeConfigurationMapping.md)
  - [DropConfigurationMapping](../D/DropConfigurationMapping.md)

## Notes and Other Information
- This is a static function, only accessible within the tsearchcmds.c file
- Returns NIL (empty list) if the input tokennames list is empty
- Validates that the parser has a defined lextype method before proceeding
- Automatically deduplicates token names to ensure each token type appears only once in the result
- Memory allocation for TSTokenTypeItem structures uses palloc0 for zero-initialized memory
- Token name strings are duplicated using pstrdup to ensure proper memory management
- Part of PostgreSQL's text search configuration management system for mapping token types

## Simplified Source

```c
static List *
getTokenTypes(Oid prsId, List *tokennames)
{
    TSParserCacheEntry *prs = lookup_ts_parser_cache(prsId);
    LexDescr *list;
    List *result = NIL;
    int ntoken;
    ListCell *tn;

    ntoken = list_length(tokennames);
    if (ntoken == 0)
        return NIL;

    // Ensure parser has lextype method defined
    if (!OidIsValid(prs->lextypeOid))
        elog(ERROR, "method lextype isn't defined for text search parser %u",
             prsId);

    // Get available token types from parser
    list = (LexDescr *) DatumGetPointer(OidFunctionCall1(prs->lextypeOid,
                                                        (Datum) 0));

    // Process each requested token name
    foreach(tn, tokennames)
    {
        String *val = lfirst_node(String, tn);
        bool found = false;
        int j;

        // Skip if already in result (deduplication)
        if (tstoken_list_member(strVal(val), result))
            continue;

        // Search for token name in parser's lexical types
        j = 0;
        while (list && list[j].lexid)
        {
            if (strcmp(strVal(val), list[j].alias) == 0)
            {
                TSTokenTypeItem *ts = palloc0(sizeof(TSTokenTypeItem));

                ts->num = list[j].lexid;
                ts->name = pstrdup(strVal(val));
                result = lappend(result, ts);
                found = true;
                break;
            }
            j++;
        }

        if (!found)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("token type \"%s\" does not exist",
                            strVal(val))));
    }

    return result;
}
```