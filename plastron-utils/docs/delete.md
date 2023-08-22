# Delete Command

Aliases: `del`, `rm`

## CLI Usage

```
usage: plastron delete [-h] [-R RECURSIVE] [-d] [--no-transactions]
                       [--completed COMPLETED] [-f FILE]
                       [uris [uris ...]]

Delete objects from the repository

positional arguments:
  uris                  Repository URIs to be deleted.

optional arguments:
  -h, --help            show this help message and exit
  -R RECURSIVE, --recursive RECURSIVE
                        Delete additional objects found by traversing the
                        given predicate(s)
  -d, --dry-run         Simulate a delete without modifying the repository
  --no-transactions, --no-txn
                        run the update without using transactions
  --completed COMPLETED
                        file recording the URIs of deleted resources
  -f FILE, --file FILE  File containing a list of URIs to delete
```

## Examples

### Delete all items in a Collection (Flat Structure)

In a "flat" structure, all the items in a collection are children of a single
parent container (typically "/pcdm"), and items and pages are siblings instead
of parent-child.  To delete all the resources associated with a single
collection:

1) In Solr, run a query:

| Field                | Value                             | Note |
| -------------------- | --------------------------------- | ---- |
| q                    | pcdm_member_of:"{COLLECTION_URI}" | where {COLLECTION_URI} is the collection URI |
| rows                 | 1000                              | a number large enough to get all the resources |
| fl                   | id                                |      |
| Raw Query Parameters | csv.header=no                     | Disables the "header" line in the CSV |
| wt                   | csv                               |      |

This will generate a list of URIs to be deleted.

2) Copy the list of URIs from the previous step, and save in a file such as
"delete_uris.txt"

3) Run the Plastron CLI:

```
$ plastron --config {CONFIG_FILE} delete --recursive "pcdm:hasMember,pcdm:hasFile,pcdm:hasRelatedObject" --file delete_uris.txt
```

where {CONFIG_FILE} is the Plastron configuration file.

**Note:** The above does _not_ delete the collection resource itself. If you
want to delete the collection resource as well, add the URI of the collection
to the list in "delete_uris.txt" or delete it using a second "delete" command.

### Delete all items in a Collection (Hierarchical Structure)

In a "hierarchical" structure, all the items in a collection are descendents of
the collection URI. Therefore, deleting a collection consists of simply deleting
the collection URI.

If the collection is going to be loaded again at the original URI, the
"tombstone" for the collection also needs to be deleted.

1) Delete the collection URI using the the "delete" command of the Plastron CLI.
The general form of the command is:

```
$ plastron --config {CONFIG_FILE} delete {COLLECTION_URI}
```

where {CONFIG_FILE} is the Plastron configuration file, and {COLLECTION_URI}
is the URI of the collection.

For example, if the configuration file is "config/localhost.yml" and the
collection URI is "http://localhost:8080/rest/dc/2016/1" the command would be:

```
$ plastron --config config/localhost.yml delete http://localhost:8080/rest/dc/2016/1
```

2) (Optional) Delete the "tombstone" resource for the collection URI using
"curl":

    2.1. Get an "auth token" accessing the fcrepo web application by going to
    "{FCREPO URL}/user/token?subject=curl&role=fedoraAdmin". For example, for
    the local development environment:

    [http://localhost:8080/user/token?subject=curl&role=fedoraAdmin](http://localhost:8080/user/token?subject=curl&role=fedoraAdmin)

    2.2. Create an "AUTH_TOKEN" environment variable:

    ```
    $ export AUTH_TOKEN={TOKEN}
    ```

    where {TOKEN} is the JWT token string returned by the previous step.

    2.3. To delete the tombstone, the general form of the command is:

    ```
    $ curl -H "Authorization: Bearer $AUTH_TOKEN" -X DELETE {COLLECTION_URI}/fcr:tombstone
    ```

    where {COLLECTION_URI} is the URI of the collection.

    Using "http://localhost:8080/rest/dc/2016/1" as our collection URI, the command
    would be:

    ```
    $ curl -H "Authorization: Bearer $AUTH_TOKEN" -X DELETE http://localhost:8080/rest/dc/2016/1/fcr:tombstone
    ```