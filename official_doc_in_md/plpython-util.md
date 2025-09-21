44.9. Utility Functions  
---  
[Prev](plpython-transactions.md "44.8. Transaction Management") | [Up](plpython.md "Chapter 44. PL/Python — Python Procedural Language")| Chapter 44. PL/Python — Python Procedural Language| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](plpython-python23.md "44.10. Python 2 vs. Python 3")  
  
* * *

## 44.9. Utility Functions #

The `plpy` module also provides the functions 

`plpy.debug(_`msg, **kwargs`_)`  
---  
`plpy.log(_`msg, **kwargs`_)`  
`plpy.info(_`msg, **kwargs`_)`  
`plpy.notice(_`msg, **kwargs`_)`  
`plpy.warning(_`msg, **kwargs`_)`  
`plpy.error(_`msg, **kwargs`_)`  
`plpy.fatal(_`msg, **kwargs`_)`  
  
`plpy.error` and `plpy.fatal` actually raise a Python exception which, if uncaught, propagates out to the calling query, causing the current transaction or subtransaction to be aborted. `raise plpy.Error(_`msg`_)` and `raise plpy.Fatal(_`msg`_)` are equivalent to calling `plpy.error(_`msg`_)` and `plpy.fatal(_`msg`_)`, respectively but the `raise` form does not allow passing keyword arguments. The other functions only generate messages of different priority levels. Whether messages of a particular priority are reported to the client, written to the server log, or both is controlled by the [log_min_messages](runtime-config-logging.md#GUC-LOG-MIN-MESSAGES) and [client_min_messages](runtime-config-client.md#GUC-CLIENT-MIN-MESSAGES) configuration variables. See [Chapter 19](runtime-config.md "Chapter 19. Server Configuration") for more information. 

The _`msg`_ argument is given as a positional argument. For backward compatibility, more than one positional argument can be given. In that case, the string representation of the tuple of positional arguments becomes the message reported to the client. 

The following keyword-only arguments are accepted: 

`detail`  
---  
`hint`  
`sqlstate`  
`schema_name`  
`table_name`  
`column_name`  
`datatype_name`  
`constraint_name`  
  
The string representation of the objects passed as keyword-only arguments is used to enrich the messages reported to the client. For example: 
    
    
    CREATE FUNCTION raise_custom_exception() RETURNS void AS $$
    plpy.error("custom exception message",
               detail="some info about exception",
               hint="hint for users")
    $$ LANGUAGE plpython3u;
    
    =# SELECT raise_custom_exception();
    ERROR:  plpy.Error: custom exception message
    DETAIL:  some info about exception
    HINT:  hint for users
    CONTEXT:  Traceback (most recent call last):
      PL/Python function "raise_custom_exception", line 4, in <module>
        hint="hint for users")
    PL/Python function "raise_custom_exception"
    

Another set of utility functions are `plpy.quote_literal(_`string`_)`, `plpy.quote_nullable(_`string`_)`, and `plpy.quote_ident(_`string`_)`. They are equivalent to the built-in quoting functions described in [Section 9.4](functions-string.md "9.4. String Functions and Operators"). They are useful when constructing ad-hoc queries. A PL/Python equivalent of dynamic SQL from [Example 41.1](plpgsql-statements.md#PLPGSQL-QUOTE-LITERAL-EXAMPLE "Example 41.1. Quoting Values in Dynamic Queries") would be: 
    
    
    plpy.execute("UPDATE tbl SET %s = %s WHERE key = %s" % (
        plpy.quote_ident(colname),
        plpy.quote_nullable(newvalue),
        plpy.quote_literal(keyvalue)))
    

* * *

[Prev](plpython-transactions.md "44.8. Transaction Management") | [Up](plpython.md "Chapter 44. PL/Python — Python Procedural Language")|  [Next](plpython-python23.md "44.10. Python 2 vs. Python 3")  
---|---|---  
44.8. Transaction Management | [Home](index.md "PostgreSQL 17.5 Documentation")|  44.10. Python 2 vs. Python 3
