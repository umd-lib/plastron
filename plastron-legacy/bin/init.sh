#!/bin/bash

# "Initializes" a Fedora 4 repository by pre-binding desired namespaces
# to chosen prefixes.  Namespace bindings must be specified in as PREFIXes 
# in the query below and also used in the INSERTed triples in order to be bound.

REST="http://localhost:8080/fcrepo/rest"
USER="fedoraAdmin"
PASSWORD="secret3"

QUERY="
    PREFIX pcdm: <http://pcdm.org/models#> 
    PREFIX bibo: <http://purl.org/ontology/bibo/>
    INSERT DATA { 
        <> a pcdm:Object ;
            bibo:shortTitle 'Fedora4 Site Initialization Page' .
        }"

curl -u $USER:$PASSWORD -X PUT "$REST/initialize/"
curl -u $USER:$PASSWORD -X PATCH -H "Content-Type: application/sparql-update" \
    --data "$QUERY" "$REST/initialize"
curl -u $USER:$PASSWORD -X DELETE "$REST/initialize"
curl -u $USER:$PASSWORD -X DELETE "$REST/initialize/fcr:tombstone"
