# 0004 - Resource Publication and Visibility via Import

Date: June 11, 2024

## Context

Plastron has historically provided the ability to control the publication and
visibility status of resources on an individual basis via the "PUBLISH" and
"HIDDEN" columns of the metadata CSV file.

As Plastron has evolved, and Archelon has gained the ability to handle imports,
controlling the publication and visibility status of resources via the
metadata CSV file has become increasingly difficult. For example, bugs have been
seen in which the per-row specification of "PUBLISH" and "HIDDEN", would work
when importing via the Plastron CLI “import” command, but would *not* work when
importing the same CSV file through Archelon.

The notion of what "published" means has also changed over time. Initially, it
simply meant that an "rdf:type" of `http://vocab.lib.umd.edu/access#Published`
was added to the resource. This eventually changed, however, so that publishing
an item also included the minting of a handle for the resource.

Because of these changes, it came to be understood that the existing usage
of "PUBLISH" and "HIDDEN" fields in the CSV file:

* Conflated the “state” of the field (whether a resource was published or
  hidden) with the “action” of publishing/hiding resources

* Was confusing, because while “True” in the appropriate column would
  publish/hide the resource, setting the column to “False” would *not*
  unpublish/unhide the resource.

## Decision

Based on the above the decision was made to remove from Plastron the ability to
publish or hide individual resources when importing a CSV file. In other words,
the “PUBLISH” and “HIDDEN” fields in the CSV file would only reflect the current
state of the resource, and changing them would have no effect on the resource.

The ability to immediately publish all the resources in an import by specifying
the “--publish” flag on the  Plastron “import” command has been retained, as it
is remains useful to importers, and its semantics are straightforward.

Plastron has `publish` and `unpublish` commands that enable a resource (or list
of resources) to be published/unpublished, and those commands also provide
options for setting the visibility (to either hidden or visible).

## Consequences

Not being able to control the publication status and visibility of individual
resources when importing may inconvenience users, as publishing/hiding resources
may require additional steps. The most common use case of immediately publishing
resources on import, however, has been retained.

The overall functionality of Plastron is unchanged, as the ability to control
the publication status and visibility of resources is provided by other
commands.
