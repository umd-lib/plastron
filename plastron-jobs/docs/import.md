# Import Jobs

Every time the import command runs, it is in the context of an _import job_. 
Plastron stores the configuration specified when running the import, and 
the source CSV file, as well as a log of successfully imported items, and 
logs of items that were dropped during a particular run.

After starting a job, you can use its job ID to resume it at a later time. When
resuming a job, plastron will check the completed log for the job and skip any
items recorded there.

The `completed.log.csv` has the following columns:

| Name        | Purpose                                                          |
|-------------|------------------------------------------------------------------|
| `id`        | Unique identifier for this item (within the context of this job) |
| `timestamp` | Date and time when this item was successfully imported           |
| `title`     | Title of the item                                                |
| `uri`       | URI of the item in the target repository                         |

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

### Example

Start a new job:

```bash
plastron -c repo.yml import \
    --model Item \
    --binaries-location /path/to/binaries \
    --member-of http://localhost:8080/rest/collections/foo \
    --container /objects \
    --job-id import-foo-1
    metadata.csv
```

Plastron will create the following structure in the `JOBS_DIR`:

```
{JOBS_DIR}
    +- import-foo-1                 # job ID
        +- config.yml               # command-line options
        +- source.csv               # copy of metadata.csv
        +- completed.log.csv        # completed item log
        +- {run1-timestamp}         # directory for the first run
            +- dropped-failed.csv   # log of failed items from this run
            +- dropped-invalid.csv  # log of invalid items from this run
```

Resume that job later:

```bash
plastron -c repo.yml import \
    --job-id import-foo-1 \
    --resume
```

A second run directory will be created:

```
{JOBS_DIR}
    +- import-foo-1                 # job ID
        +- config.yml               # command-line options
        +- source.csv               # copy of metadata.csv
        +- completed.log.csv        # completed item log
        +- {run1-timestamp}         # directory for the first run
        |   +- dropped-failed.csv   # log of failed items from this run
        |   +- dropped-invalid.csv  # log of invalid items from this run
        +- {run2-timestamp}         # directory for the second run
            +- dropped-failed.csv   # log of failed items from this run
            +- dropped-invalid.csv  # log of invalid items from this run
```

## Percentage Imports

You may use the `-%` or `--percent` option to import only a subset of the items
in the import metadata CSV. Repeated use of this option with the same job will
select new subsets of items that have not yet been imported.

For example, start a job that has 50 items total, but only load 10% at first:

```bash
plastron -c repo.yml import \
    --model Item \
    --binaries-location /path/to/binaries \
    --member-of http://localhost:8080/rest/collections/foo \
    --container /objects \
    --job-id percentile-job \
    --percent 10
```

Plastron will only import 5 items (10% of 50), as evenly spaced within the set of
uncompleted items as possible.

If you resume the job with the `--percent 10` option again:

```bash
plastron -c repo.yml import \
    --job-id percentile-job \
    --resume \
    --percent 10
```

Plastron will import 5 more items, selected from the 45 items that were not
imported during the first run of the job.

If you specify a percentage that would generate a subset larger than the number
of remaining items, Plastron will import all the remaining items.

## Metadata Spreadsheet

### `FILES` column format

In the metadata spreadsheet CSV file, the `FILES` column (if present) 
specifies both the binary files that should be included with the object, 
and also their ordering into a sequence of member objects of the main object.

* The `FILES` column of an import spreadsheet may have zero or more 
  relpaths (relative paths), separated by semicolons (e.g., 
  `ex-99/ex-99-0001.tif;ex-99/ex-99-0001.jpg;ex-99/ex-99-0002.tif;ex-99/ex-99-0002.jpg`)
* A relpath is a relative path to an individual file (e.g., 
  `ex-99/ex-99-0001.tif`)
* The filename is the right-most segment of the relpath (e.g., 
  `ex-99-0001.tif`)
* The basename is the filename, excluding the file extension and its period 
  separator (e.g., `ex-99-0001`)
* All relpaths that share a basename are considered to be part of a single 
  file group
* Each file group corresponds to one member object
* Each relpath within a file group will be used to create a `pcdm:File` 
  object for that file group's member object
* The member objects will be added to a sequence in the order in which 
  their basename first occurs in the `FILES` field

By default, the member objects have titles of the form "Page 1" to "Page 
N". You may also specify custom labels in the relpath list to get custom 
titles on the member objects.

* A relpath may have an initial string followed by a colon; this is a label 
  for the page for files in its file group (e.g.,
  `Front Cover:ex-99/ex-99-0001.tif`)
* If using a label, only one relpath in a file group is required to have that 
  label
* If more than one relpath from a single file group has a label, those labels 
  must match; mismatched labels will cause the validation of the item to fail
* If at least one relpath has a label, then each file group must have at 
  least one relpath with a label; otherwise this will cause validation of 
  the item to fail
* If no relpaths have labels, the default behavior of assigning the labels 
  "Page 1" through "Page N" based on the document order of the relpaths 
  will be used
