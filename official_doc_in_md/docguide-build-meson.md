J.4. Building the Documentation with Meson  
---  
[Prev](docguide-build.md "J.3. Building the Documentation with Make") | [Up](docguide.md "Appendix J. Documentation")| Appendix J. Documentation| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](docguide-authoring.md "J.5. Documentation Authoring")  
  
* * *

## J.4. Building the Documentation with Meson #

To build the documentation using Meson, change to the `build` directory before running one of these commands, or add `-C build` to the command. 

To build just the HTML version of the documentation: 
    
    
    build$ **ninja html**
    

For a list of other documentation targets see [Section 17.4.4.3](install-meson.md#TARGETS-MESON-DOCUMENTATION "17.4.4.3. Documentation Targets"). The output appears in the subdirectory `build/doc/src/sgml`. 

* * *

[Prev](docguide-build.md "J.3. Building the Documentation with Make") | [Up](docguide.md "Appendix J. Documentation")|  [Next](docguide-authoring.md "J.5. Documentation Authoring")  
---|---|---  
J.3. Building the Documentation with Make | [Home](index.md "PostgreSQL 17.5 Documentation")|  J.5. Documentation Authoring
