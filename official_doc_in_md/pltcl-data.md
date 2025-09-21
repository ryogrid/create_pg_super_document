42.3. Data Values in PL/Tcl  
---  
[Prev](pltcl-functions.md "42.2. PL/Tcl Functions and Arguments") | [Up](pltcl.md "Chapter 42. PL/Tcl — Tcl Procedural Language")| Chapter 42. PL/Tcl — Tcl Procedural Language| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](pltcl-global.md "42.4. Global Data in PL/Tcl")  
  
* * *

## 42.3. Data Values in PL/Tcl #

The argument values supplied to a PL/Tcl function's code are simply the input arguments converted to text form (just as if they had been displayed by a `SELECT` statement). Conversely, the `return` and `return_next` commands will accept any string that is acceptable input format for the function's declared result type, or for the specified column of a composite result type. 

* * *

[Prev](pltcl-functions.md "42.2. PL/Tcl Functions and Arguments") | [Up](pltcl.md "Chapter 42. PL/Tcl — Tcl Procedural Language")|  [Next](pltcl-global.md "42.4. Global Data in PL/Tcl")  
---|---|---  
42.2. PL/Tcl Functions and Arguments | [Home](index.md "PostgreSQL 17.5 Documentation")|  42.4. Global Data in PL/Tcl
