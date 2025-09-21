ALTER TEXT SEARCH DICTIONARY  
---  
[Prev](sql-altertsconfig.md "ALTER TEXT SEARCH CONFIGURATION") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-altertsparser.md "ALTER TEXT SEARCH PARSER")  
  
* * *

## ALTER TEXT SEARCH DICTIONARY

ALTER TEXT SEARCH DICTIONARY — change the definition of a text search dictionary

## Synopsis
    
    
    ALTER TEXT SEARCH DICTIONARY _name_ (
        _option_ [ = _value_ ] [, ... ]
    )
    ALTER TEXT SEARCH DICTIONARY _name_ RENAME TO _new_name_
    ALTER TEXT SEARCH DICTIONARY _name_ OWNER TO { _new_owner_ | CURRENT_ROLE | CURRENT_USER | SESSION_USER }
    ALTER TEXT SEARCH DICTIONARY _name_ SET SCHEMA _new_schema_
    

## Description

`ALTER TEXT SEARCH DICTIONARY` changes the definition of a text search dictionary. You can change the dictionary's template-specific options, or change the dictionary's name or owner. 

You must be the owner of the dictionary to use `ALTER TEXT SEARCH DICTIONARY`. 

## Parameters

 _`name`_
    

The name (optionally schema-qualified) of an existing text search dictionary. 

_`option`_
    

The name of a template-specific option to be set for this dictionary. 

_`value`_
    

The new value to use for a template-specific option. If the equal sign and value are omitted, then any previous setting for the option is removed from the dictionary, allowing the default to be used. 

_`new_name`_
    

The new name of the text search dictionary. 

_`new_owner`_
    

The new owner of the text search dictionary. 

_`new_schema`_
    

The new schema for the text search dictionary. 

Template-specific options can appear in any order. 

## Examples

The following example command changes the stopword list for a Snowball-based dictionary. Other parameters remain unchanged. 
    
    
    ALTER TEXT SEARCH DICTIONARY my_dict ( StopWords = newrussian );
    

The following example command changes the language option to `dutch`, and removes the stopword option entirely. 
    
    
    ALTER TEXT SEARCH DICTIONARY my_dict ( language = dutch, StopWords );
    

The following example command “updates” the dictionary's definition without actually changing anything. 
    
    
    ALTER TEXT SEARCH DICTIONARY my_dict ( dummy );
    

(The reason this works is that the option removal code doesn't complain if there is no such option.) This trick is useful when changing configuration files for the dictionary: the `ALTER` will force existing database sessions to re-read the configuration files, which otherwise they would never do if they had read them earlier. 

## Compatibility

There is no `ALTER TEXT SEARCH DICTIONARY` statement in the SQL standard. 

## See Also

[CREATE TEXT SEARCH DICTIONARY](sql-createtsdictionary.md "CREATE TEXT SEARCH DICTIONARY"), [DROP TEXT SEARCH DICTIONARY](sql-droptsdictionary.md "DROP TEXT SEARCH DICTIONARY")

* * *

[Prev](sql-altertsconfig.md "ALTER TEXT SEARCH CONFIGURATION") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-altertsparser.md "ALTER TEXT SEARCH PARSER")  
---|---|---  
ALTER TEXT SEARCH CONFIGURATION | [Home](index.md "PostgreSQL 17.5 Documentation")|  ALTER TEXT SEARCH PARSER
