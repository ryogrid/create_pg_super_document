F.12. dict_int — example full-text search dictionary for integers  
---  
[Prev](contrib-dblink-build-sql-update.md "dblink_build_sql_update") | [Up](contrib.md "Appendix F. Additional Supplied Modules and Extensions")| Appendix F. Additional Supplied Modules and Extensions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](dict-xsyn.md "F.13. dict_xsyn — example synonym full-text search dictionary")  
  
* * *

## F.12. dict_int — example full-text search dictionary for integers #

[F.12.1. Configuration](dict-int.md#DICT-INT-CONFIG)
[F.12.2. Usage](dict-int.md#DICT-INT-USAGE)

`dict_int` is an example of an add-on dictionary template for full-text search. The motivation for this example dictionary is to control the indexing of integers (signed and unsigned), allowing such numbers to be indexed while preventing excessive growth in the number of unique words, which greatly affects the performance of searching. 

This module is considered “trusted”, that is, it can be installed by non-superusers who have `CREATE` privilege on the current database. 

### F.12.1. Configuration #

The dictionary accepts three options: 

  * The `maxlen` parameter specifies the maximum number of digits allowed in an integer word. The default value is 6. 

  * The `rejectlong` parameter specifies whether an overlength integer should be truncated or ignored. If `rejectlong` is `false` (the default), the dictionary returns the first `maxlen` digits of the integer. If `rejectlong` is `true`, the dictionary treats an overlength integer as a stop word, so that it will not be indexed. Note that this also means that such an integer cannot be searched for. 

  * The `absval` parameter specifies whether leading “`+`” or “`-`” signs should be removed from integer words. The default is `false`. When `true`, the sign is removed before `maxlen` is applied. 




### F.12.2. Usage #

Installing the `dict_int` extension creates a text search template `intdict_template` and a dictionary `intdict` based on it, with the default parameters. You can alter the parameters, for example 
    
    
    mydb# ALTER TEXT SEARCH DICTIONARY intdict (MAXLEN = 4, REJECTLONG = true);
    ALTER TEXT SEARCH DICTIONARY
    

or create new dictionaries based on the template. 

To test the dictionary, you can try 
    
    
    mydb# select ts_lexize('intdict', '12345678');
     ts_lexize
    -----------
     {123456}
    

but real-world usage will involve including it in a text search configuration as described in [Chapter 12](textsearch.md "Chapter 12. Full Text Search"). That might look like this: 
    
    
    ALTER TEXT SEARCH CONFIGURATION english
        ALTER MAPPING FOR int, uint WITH intdict;
    

* * *

[Prev](contrib-dblink-build-sql-update.md "dblink_build_sql_update") | [Up](contrib.md "Appendix F. Additional Supplied Modules and Extensions")|  [Next](dict-xsyn.md "F.13. dict_xsyn — example synonym full-text search dictionary")  
---|---|---  
dblink_build_sql_update | [Home](index.md "PostgreSQL 17.5 Documentation")|  F.13. dict_xsyn — example synonym full-text search dictionary
