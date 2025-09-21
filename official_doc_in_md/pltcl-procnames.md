42.12. Tcl Procedure Names  
---  
[Prev](pltcl-config.md "42.11. PL/Tcl Configuration") | [Up](pltcl.md "Chapter 42. PL/Tcl — Tcl Procedural Language")| Chapter 42. PL/Tcl — Tcl Procedural Language| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](plperl.md "Chapter 43. PL/Perl — Perl Procedural Language")  
  
* * *

## 42.12. Tcl Procedure Names #

In PostgreSQL, the same function name can be used for different function definitions as long as the number of arguments or their types differ. Tcl, however, requires all procedure names to be distinct. PL/Tcl deals with this by making the internal Tcl procedure names contain the object ID of the function from the system table `pg_proc` as part of their name. Thus, PostgreSQL functions with the same name and different argument types will be different Tcl procedures, too. This is not normally a concern for a PL/Tcl programmer, but it might be visible when debugging. 

* * *

[Prev](pltcl-config.md "42.11. PL/Tcl Configuration") | [Up](pltcl.md "Chapter 42. PL/Tcl — Tcl Procedural Language")|  [Next](plperl.md "Chapter 43. PL/Perl — Perl Procedural Language")  
---|---|---  
42.11. PL/Tcl Configuration | [Home](index.md "PostgreSQL 17.5 Documentation")|  Chapter 43. PL/Perl — Perl Procedural Language
