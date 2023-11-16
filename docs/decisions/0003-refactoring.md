# 0003 - Split Plastron into Multiple Packages

* ID: plastron-0003
* Date: 16 November 2023
* Author: Peter Eichman

## Background

The Plastron project grew out of the earlier `newspaper-batchloader` project
developed by Josh Westgard. Since then, Plastron has gradually accrued many
more features beyond its initial scope as a command-line batch loader. These
include such things as RDF data modeling and validation, CSV-RDF translation,
a STOMP client for processing asynchronous requests, an HTTP server for
handling certain synchronous requests, and more.

## Problem

At this point, the Plastron code base has become large and unwieldy. There are
also components that would be ideal for re-use in other projects (e.g., Fedora
authentication and connection in OAI-PMH server) but there is no way to
include them without including the whole Plastron project as a dependency.
This creates complications with dependencies, as Plastron currently has some
outdated ones listed in its setup.py. While these should be cleaned up, the
monolithic nature of the current codebase and lack of test coverage for some
parts of the code mean that updating dependency versions would be a
potentially fraught and perilous process.

## Solution

Split the Plastron codebase into multiple packages. Each package should aim to
cover one area of functionality of the current project, and delegate to the
other Plastron packages when appropriate.

## Proposed Packages

* **LDP client (plastron-client)** repository endpoints, HTTP requests,
  authentication, transaction handling
* **Repository operations (plastron-repo)** loading and storing data at a
  Digital Object or higher level of abstraction; possibly structural
  modeling? (flat vs. hierarchical, object creators)
* **Content modeling (plastron-models)** RDF to Python conversion, CSV to RDF
  conversion, validation
* **STOMP services (plastron-stomp)** the asynchronous STOMP messaging client
* **HTTP services (plastron-web)** the HTTP webapp
* **CLI services (plastron-cli)** the command-line interface

## Implementation

As each new package is developed, the current Plastron codebase can be
modified to include that package as a dependency, and remove the code from
itself. The intent is, once all the new packages are complete, the original
Plastron codebase would be a “meta-repository” that pulls all the packages
together, and provides shortcuts for installing and using the whole Plastron
stack, but does not have any substantial code of its own.

## Versioning

The main Plastron codebase and the new packages would all have the version
4.0.0 for their initial release. After that, it remains to be determined
whether we would like to keep the versions of each of the packages in
lock-step with one another, or if we should allow them to naturally drift.

# History

- 2023-11-16: Revised the text of the original proposal into this document
- 2023-05-16: Original proposal: <https://docs.google.com/document/d/1GtcJ3Qh5GCBPlGoCKCDYLHAr8jxsiHOby0_OBfGwXY0/edit>