# Message Formats

The Plastron Daemon emits both incremental progress messages, and a final job
completion message. The headers of these messages are standardized across
commands, but the bodies have command-specific formats.

## Headers

| Header             | Message Type         | Usage                                           |
|--------------------|----------------------|-------------------------------------------------|
| `PlastronJobId`    | progress, completion | URI that identifies the job                     |
| `PlastronJobState` | completion           | Job state as of the end of this run             |
| `PlastronJobError` | completion           | Error message, if the job had a fatal exception |

## Bodies

All message bodies are JSON-formatted.

### Export

#### Progress message

```
{
    "time": {
        "started": /* job start time expressed as unix timestamp */,
        "now": /* current time expressed as a unix timestamp */,
        "elapsed": /* number of seconds the job has been running */
    },
    "count": {
        "total": /* total number of resources to export */,
        "exported": /* number of resources successfully exported so far */,
        "errors": /* number of resources that could not be exported due to errors */
    }
}
```

#### Completion message

The `PlastronJobState` header is copied from the `"type"` field in the message body.

```
{
    "type": "export_complete" | "partial_export",
    "content_type": /* MIME type of the export file: text/turtle, text/csv, or application/zip */,
    "file_extension": /* file extension of the export output file: .ttl, .csv, or .zip */,
    "download_uri": /* repository URI of the export file */
    "count": {
        "total": /* total number of resources to export */,
        "exported": /* number of resources successfully exported */,
        "errors": /* number of resources that could not be exported due to errors */
    }
}
```

### Import

In the import messages, there are two distinct sets of counts:

* updated/unchanged
* valid/invalid

Also, if the system is unable to determine ahead of time the number of
rows in the import file (e.g., it is not a seek-able stream), then the
`count.total` in the progress messages will be `null`. The `count.total`
in the completion message is updated with the actual number of rows
counted during the import processing.

#### Progress message

```
{
    "time": {
        "started": start_time,
        "now": now,
        "elapsed": now - start_time
    },
    "count": {
        "total": /* total number of resources to import */,
        "updated": /* number of resources with changes that have been updated so far */,
        "unchanged": /* number of resources with no changes so far */,
        "valid": /* number of resources that have passed validation checks so far */,
        "invalid": /* number of resources that have failed validation checks so far */,
        "errors": /* number of resources that could not be imported due to errors */
    }
}
```

#### Completion message

The `PlastronJobState` header is copied from the `"type"` field in the message body.

```
{
    "type": "validate_success" | "validate_failed" | "import_complete" | "import_incomplete",
    "count": {
        "total": /* total number of resources to import */,
        "updated": /* number of resources with changes that were updated */,
        "unchanged": /* number of resources with no changes */,
        "valid": /* number of resources that passed validation checks */,
        "invalid": /* number of resources that failed validation checks */,
        "errors": /* number of resources that could not be imported due to errors */
    },
    "validation": [
        /* list of validation result reports for each row and each field */
        {
            "line": /* line reference in the form "filename:line number" */,
            "is_valid": /* true or false */,
            "passed": [
                /* list of fields that passed validation */
                [ field_name, "passed", rule_name, rule_argument ],
                [ field_name, "passed", rule_name, rule_argument ],
                /* ... */
            ],
            "failed": [
                /* list of fields that passed validation */
                [ field_name, "failed", rule_name, rule_argument ],
                [ field_name, "failed", rule_name, rule_argument ],
                /* ... */
            ]
        },
        { /* additional report */ },
        /* ... */
    ]
}
```
