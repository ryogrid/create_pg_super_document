44.3. Sharing Data  
---  
[Prev](plpython-data.md "44.2. Data Values") | [Up](plpython.md "Chapter 44. PL/Python — Python Procedural Language")| Chapter 44. PL/Python — Python Procedural Language| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](plpython-do.md "44.4. Anonymous Code Blocks")  
  
* * *

## 44.3. Sharing Data #

The global dictionary `SD` is available to store private data between repeated calls to the same function. The global dictionary `GD` is public data, that is available to all Python functions within a session; use with care.

Each function gets its own execution environment in the Python interpreter, so that global data and function arguments from `myfunc` are not available to `myfunc2`. The exception is the data in the `GD` dictionary, as mentioned above. 

* * *

[Prev](plpython-data.md "44.2. Data Values") | [Up](plpython.md "Chapter 44. PL/Python — Python Procedural Language")|  [Next](plpython-do.md "44.4. Anonymous Code Blocks")  
---|---|---  
44.2. Data Values | [Home](index.md "PostgreSQL 17.5 Documentation")|  44.4. Anonymous Code Blocks
