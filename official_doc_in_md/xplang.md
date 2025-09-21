Chapter 40. Procedural Languages  
---  
[Prev](rules-triggers.md "39.7. Rules Versus Triggers") | [Up](server-programming.md "Part V. Server Programming")| Part V. Server Programming| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](xplang-install.md "40.1. Installing Procedural Languages")  
  
* * *

## Chapter 40. Procedural Languages

**Table of Contents**

[40.1. Installing Procedural Languages](xplang-install.md)

PostgreSQL allows user-defined functions to be written in other languages besides SQL and C. These other languages are generically called _procedural languages_ (PLs). For a function written in a procedural language, the database server has no built-in knowledge about how to interpret the function's source text. Instead, the task is passed to a special handler that knows the details of the language. The handler could either do all the work of parsing, syntax analysis, execution, etc. itself, or it could serve as “glue” between PostgreSQL and an existing implementation of a programming language. The handler itself is a C language function compiled into a shared object and loaded on demand, just like any other C function. 

There are currently four procedural languages available in the standard PostgreSQL distribution: PL/pgSQL ([Chapter 41](plpgsql.md "Chapter 41. PL/pgSQL — SQL Procedural Language")), PL/Tcl ([Chapter 42](pltcl.md "Chapter 42. PL/Tcl — Tcl Procedural Language")), PL/Perl ([Chapter 43](plperl.md "Chapter 43. PL/Perl — Perl Procedural Language")), and PL/Python ([Chapter 44](plpython.md "Chapter 44. PL/Python — Python Procedural Language")). There are additional procedural languages available that are not included in the core distribution. [Appendix H](external-projects.md "Appendix H. External Projects") has information about finding them. In addition other languages can be defined by users; the basics of developing a new procedural language are covered in [Chapter 56](plhandler.md "Chapter 56. Writing a Procedural Language Handler"). 

* * *

[Prev](rules-triggers.md "39.7. Rules Versus Triggers") | [Up](server-programming.md "Part V. Server Programming")|  [Next](xplang-install.md "40.1. Installing Procedural Languages")  
---|---|---  
39.7. Rules Versus Triggers | [Home](index.md "PostgreSQL 17.5 Documentation")|  40.1. Installing Procedural Languages
