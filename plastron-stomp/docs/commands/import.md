# Import Command

STOMP message headers:

```
PlastronCommand: import
PlastronJobId: JOB_ID
PlastronArg-model: MODEL
PlastronArg-limit: LIMIT
PlastronArg-percent: PERCENTAGE
PlastronArg-validate-only: {true|false}
PlastronArg-publish: {true|false}
PlastronArg-resume: {true|false}
PlastronArg-access: ACCESS
PlastronArg-member-of: MEMBER_OF
PlastronArg-binaries-location: BINARIES_LOCATION
PlastronArg-container: CONTAINER
PlastronArg-extract-text: MIME_TYPES
PlastronArg-structure: {flat|hierarchical}
PlastronArg-relpath: PATH
```

## Configuration

The following keys are used in the `COMMANDS/IMPORT` section of the config file:

| Name              | Purpose                                                                                          |
|-------------------|--------------------------------------------------------------------------------------------------|
| `JOBS_DIR`        | Base directory for storing [job](#jobs) information. Defaults to `jobs` in the working directory |
| `SSH_PRIVATE_KEY` | Path to the private key to use when retrieving binaries over SFTP                                |

## Jobs

Every time the import command runs, it is in the context of a _job_. Plastron
stores the configuration specified when running the import, and the source CSV
file, as well as a log of successfully imported items, and logs of items that
were dropped during a particular run.

After starting a job, you can use its job ID to resume it at a later time. When
resuming a job, plastron will check the completed log for the job and skip any
items recorded there.

The `completed.log.csv` has the following columns:

| Name         | Purpose                                                          |
|--------------|------------------------------------------------------------------|
| `id`         | Unique identifier for this item (within the context of this job) |
| `timestamp`  | Date and time when this item was successfully imported           |
| `title`      | Title of the item                                                |
| `uri`        | URI of the item in the target repository                         |

You may specify a job ID on the command line using the `--job-id` argument. If
you do not provide one, Plastron will generate one using the current timestamp.

### Import Failures

Items that cannot be imported during a run are categorized as either
"invalid" or "failed".

#### Invalid Items

Invalid items are items that fail metadata validation, and are recorded in
the "dropped-invalid" log for that run, along with the reason for the failure.

Invalid items will likely require changes to the source CSV file, or some other
action on the part of the user (such as adding missing files).

#### Failed Items

Failed items are items that could not be imported due to problems adding
records to the repository, and are recorded in the "dropped-failed" log for that
run, along with the reason for the failure.

Some failures may occur due to transient network issues. In those cases,
resuming the import should allow those items to tbe added.

### Dropped Item Logs

Both the "dropped-invalid" and "dropped-failed" item logs have the following
columns:

| Name        | Purpose                                                                        |
|-------------|--------------------------------------------------------------------------------|
| `id`        | Unique identifier for this item (within the context of this job)               |
| `timestamp` | Date and time when this item failed to import                                  |
| `title`     | Title of the item                                                              |
| `uri`       | URI of the item in the target repository; this may be empty if the item is new |
| `reason`    | Short description of the error leading to failure to import                    |
