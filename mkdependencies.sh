#!/bin/bash

VERSION=$(cat VERSION)

cat >dependencies.txt <<END
plastron-client==$VERSION
plastron-messaging==$VERSION
plastron-models==$VERSION
plastron-rdf==$VERSION
plastron-repo==$VERSION
plastron-utils==$VERSION
END

cat >dependencies-cli.txt <<END
plastron-cli==$VERSION
END

cat >dependencies-stomp.txt <<END
plastron-stomp==$VERSION
END

cat >dependencies-web.txt <<END
plastron-web==$VERSION
END
