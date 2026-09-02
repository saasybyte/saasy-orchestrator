#!/bin/bash

tmpdir=$(mktemp -d /tmp/secrets-XXXXXX)
trap "rm -rf $tmpdir" EXIT

if [ -n "$GCP_SA_JSON" ]; then
    echo "$GCP_SA_JSON" > "$tmpdir/gcp-creds.json"
    export GOOGLE_APPLICATION_CREDENTIALS="$tmpdir/gcp-creds.json"
fi

exec "$@"
