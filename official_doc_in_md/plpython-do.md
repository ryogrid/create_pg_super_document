44.4. Anonymous Code Blocks  
---  
[Prev](plpython-sharing.md "44.3. Sharing Data") | [Up](plpython.md "Chapter 44. PL/Python — Python Procedural Language")| Chapter 44. PL/Python — Python Procedural Language| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](plpython-trigger.md "44.5. Trigger Functions")  
  
* * *

## 44.4. Anonymous Code Blocks #

PL/Python also supports anonymous code blocks called with the [DO](sql-do.md "DO") statement: 
    
    
    DO $$
        # PL/Python code
    $$ LANGUAGE plpython3u;
    

An anonymous code block receives no arguments, and whatever value it might return is discarded. Otherwise it behaves just like a function. 

* * *

[Prev](plpython-sharing.md "44.3. Sharing Data") | [Up](plpython.md "Chapter 44. PL/Python — Python Procedural Language")|  [Next](plpython-trigger.md "44.5. Trigger Functions")  
---|---|---  
44.3. Sharing Data | [Home](index.md "PostgreSQL 17.5 Documentation")|  44.5. Trigger Functions
