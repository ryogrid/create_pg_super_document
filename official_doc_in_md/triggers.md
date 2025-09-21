Chapter 37. Triggers  
---  
[Prev](extend-pgxs.md "36.18. Extension Building Infrastructure") | [Up](server-programming.md "Part V. Server Programming")| Part V. Server Programming| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](trigger-definition.md "37.1. Overview of Trigger Behavior")  
  
* * *

## Chapter 37. Triggers

**Table of Contents**

[37.1. Overview of Trigger Behavior](trigger-definition.md)
[37.2. Visibility of Data Changes](trigger-datachanges.md)
[37.3. Writing Trigger Functions in C](trigger-interface.md)
[37.4. A Complete Trigger Example](trigger-example.md)

This chapter provides general information about writing trigger functions. Trigger functions can be written in most of the available procedural languages, including PL/pgSQL ([Chapter 41](plpgsql.md "Chapter 41. PL/pgSQL — SQL Procedural Language")), PL/Tcl ([Chapter 42](pltcl.md "Chapter 42. PL/Tcl — Tcl Procedural Language")), PL/Perl ([Chapter 43](plperl.md "Chapter 43. PL/Perl — Perl Procedural Language")), and PL/Python ([Chapter 44](plpython.md "Chapter 44. PL/Python — Python Procedural Language")). After reading this chapter, you should consult the chapter for your favorite procedural language to find out the language-specific details of writing a trigger in it. 

It is also possible to write a trigger function in C, although most people find it easier to use one of the procedural languages. It is not currently possible to write a trigger function in the plain SQL function language. 

* * *

[Prev](extend-pgxs.md "36.18. Extension Building Infrastructure") | [Up](server-programming.md "Part V. Server Programming")|  [Next](trigger-definition.md "37.1. Overview of Trigger Behavior")  
---|---|---  
36.18. Extension Building Infrastructure | [Home](index.md "PostgreSQL 17.5 Documentation")|  37.1. Overview of Trigger Behavior
