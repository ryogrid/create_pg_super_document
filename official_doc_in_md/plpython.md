Chapter 44. PL/Python — Python Procedural Language  
---  
[Prev](plperl-under-the-hood.md "43.8. PL/Perl Under the Hood") | [Up](server-programming.md "Part V. Server Programming")| Part V. Server Programming| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](plpython-funcs.md "44.1. PL/Python Functions")  
  
* * *

## Chapter 44. PL/Python — Python Procedural Language

**Table of Contents**

[44.1. PL/Python Functions](plpython-funcs.md)
[44.2. Data Values](plpython-data.md)
    

[44.2.1. Data Type Mapping](plpython-data.md#PLPYTHON-DATA-TYPE-MAPPING)
[44.2.2. Null, None](plpython-data.md#PLPYTHON-DATA-NULL)
[44.2.3. Arrays, Lists](plpython-data.md#PLPYTHON-ARRAYS)
[44.2.4. Composite Types](plpython-data.md#PLPYTHON-DATA-COMPOSITE-TYPES)
[44.2.5. Set-Returning Functions](plpython-data.md#PLPYTHON-DATA-SET-RETURNING-FUNCS)
[44.3. Sharing Data](plpython-sharing.md)
[44.4. Anonymous Code Blocks](plpython-do.md)
[44.5. Trigger Functions](plpython-trigger.md)
[44.6. Database Access](plpython-database.md)
    

[44.6.1. Database Access Functions](plpython-database.md#PLPYTHON-DATABASE-ACCESS-FUNCS)
[44.6.2. Trapping Errors](plpython-database.md#PLPYTHON-TRAPPING)
[44.7. Explicit Subtransactions](plpython-subtransaction.md)
    

[44.7.1. Subtransaction Context Managers](plpython-subtransaction.md#PLPYTHON-SUBTRANSACTION-CONTEXT-MANAGERS)
[44.8. Transaction Management](plpython-transactions.md)
[44.9. Utility Functions](plpython-util.md)
[44.10. Python 2 vs. Python 3](plpython-python23.md)
[44.11. Environment Variables](plpython-envar.md)

The PL/Python procedural language allows PostgreSQL functions and procedures to be written in the [Python language](https://www.python.org). 

To install PL/Python in a particular database, use `CREATE EXTENSION plpython3u`. 

### Tip

If a language is installed into `template1`, all subsequently created databases will have the language installed automatically. 

PL/Python is only available as an “untrusted” language, meaning it does not offer any way of restricting what users can do in it and is therefore named `plpython3u`. A trusted variant `plpython` might become available in the future if a secure execution mechanism is developed in Python. The writer of a function in untrusted PL/Python must take care that the function cannot be used to do anything unwanted, since it will be able to do anything that could be done by a user logged in as the database administrator. Only superusers can create functions in untrusted languages such as `plpython3u`. 

### Note

Users of source packages must specially enable the build of PL/Python during the installation process. (Refer to the installation instructions for more information.) Users of binary packages might find PL/Python in a separate subpackage. 

* * *

[Prev](plperl-under-the-hood.md "43.8. PL/Perl Under the Hood") | [Up](server-programming.md "Part V. Server Programming")|  [Next](plpython-funcs.md "44.1. PL/Python Functions")  
---|---|---  
43.8. PL/Perl Under the Hood | [Home](index.md "PostgreSQL 17.5 Documentation")|  44.1. PL/Python Functions
