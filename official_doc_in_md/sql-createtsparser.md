CREATE TEXT SEARCH PARSER  
---  
[Prev](sql-createtsdictionary.md "CREATE TEXT SEARCH DICTIONARY") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-createtstemplate.md "CREATE TEXT SEARCH TEMPLATE")  
  
* * *

## CREATE TEXT SEARCH PARSER

CREATE TEXT SEARCH PARSER — define a new text search parser

## Synopsis
    
    
    CREATE TEXT SEARCH PARSER _name_ (
        START = _start_function_ ,
        GETTOKEN = _gettoken_function_ ,
        END = _end_function_ ,
        LEXTYPES = _lextypes_function_
        [, HEADLINE = _headline_function_ ]
    )
    

## Description

`CREATE TEXT SEARCH PARSER` creates a new text search parser. A text search parser defines a method for splitting a text string into tokens and assigning types (categories) to the tokens. A parser is not particularly useful by itself, but must be bound into a text search configuration along with some text search dictionaries to be used for searching. 

If a schema name is given then the text search parser is created in the specified schema. Otherwise it is created in the current schema. 

You must be a superuser to use `CREATE TEXT SEARCH PARSER`. (This restriction is made because an erroneous text search parser definition could confuse or even crash the server.) 

Refer to [Chapter 12](textsearch.md "Chapter 12. Full Text Search") for further information. 

## Parameters

 _`name`_
    

The name of the text search parser to be created. The name can be schema-qualified. 

_`start_function`_
    

The name of the start function for the parser. 

_`gettoken_function`_
    

The name of the get-next-token function for the parser. 

_`end_function`_
    

The name of the end function for the parser. 

_`lextypes_function`_
    

The name of the lextypes function for the parser (a function that returns information about the set of token types it produces). 

_`headline_function`_
    

The name of the headline function for the parser (a function that summarizes a set of tokens). 

The function names can be schema-qualified if necessary. Argument types are not given, since the argument list for each type of function is predetermined. All except the headline function are required. 

The arguments can appear in any order, not only the one shown above. 

## Compatibility

There is no `CREATE TEXT SEARCH PARSER` statement in the SQL standard. 

## See Also

[ALTER TEXT SEARCH PARSER](sql-altertsparser.md "ALTER TEXT SEARCH PARSER"), [DROP TEXT SEARCH PARSER](sql-droptsparser.md "DROP TEXT SEARCH PARSER")

* * *

[Prev](sql-createtsdictionary.md "CREATE TEXT SEARCH DICTIONARY") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-createtstemplate.md "CREATE TEXT SEARCH TEMPLATE")  
---|---|---  
CREATE TEXT SEARCH DICTIONARY | [Home](index.md "PostgreSQL 17.5 Documentation")|  CREATE TEXT SEARCH TEMPLATE
