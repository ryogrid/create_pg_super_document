J.5. Documentation Authoring  
---  
[Prev](docguide-build-meson.md "J.4. Building the Documentation with Meson") | [Up](docguide.md "Appendix J. Documentation")| Appendix J. Documentation| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](docguide-style.md "J.6. Style Guide")  
  
* * *

## J.5. Documentation Authoring #

[J.5.1. Emacs](docguide-authoring.md#DOCGUIDE-AUTHORING-EMACS)

The documentation sources are most conveniently modified with an editor that has a mode for editing XML, and even more so if it has some awareness of XML schema languages so that it can know about DocBook syntax specifically. 

Note that for historical reasons the documentation source files are named with an extension `.sgml` even though they are now XML files. So you might need to adjust your editor configuration to set the correct mode. 

### J.5.1. Emacs #

nXML Mode, which ships with Emacs, is the most common mode for editing XML documents with Emacs. It will allow you to use Emacs to insert tags and check markup consistency, and it supports DocBook out of the box. Check the [ nXML manual](https://www.gnu.org/software/emacs/manual/html_mono/nxml-mode.md) for detailed documentation. 

`src/tools/editors/emacs.samples` contains recommended settings for this mode. 

* * *

[Prev](docguide-build-meson.md "J.4. Building the Documentation with Meson") | [Up](docguide.md "Appendix J. Documentation")|  [Next](docguide-style.md "J.6. Style Guide")  
---|---|---  
J.4. Building the Documentation with Meson | [Home](index.md "PostgreSQL 17.5 Documentation")|  J.6. Style Guide
