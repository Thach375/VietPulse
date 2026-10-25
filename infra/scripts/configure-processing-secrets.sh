#!/usr/bin/env bash
set +xv
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"
foundation_init "$@"
if $dry_run; then
    printf 'DRY RUN: copy the existing MongoDB password and ECK password/public CA into processing Secrets through encrypted stdin.\n'
    exit 0
fi
cluster_gate
on_node 1 <<'REMOTE'
for name in processing-mongodb processing-es; do
    owner=$(k -n processing get secret "$name" --ignore-not-found -o 'jsonpath={.metadata.labels.vietpulse\.io/managed}')
    exists=$(k -n processing get secret "$name" --ignore-not-found -o name)
    [[ -z $exists || $owner == processing ]] || fail "Secret $name belongs to another deployment"
done
k -n serving get secret vietpulse-mongodb-password vietpulse-es-es-elastic-user vietpulse-es-es-http-certs-public -o json | \
python3 -c '
import json,sys
items={s["metadata"]["name"]:s["data"] for s in json.load(sys.stdin)["items"]}
result=[]
for name,data in (("processing-mongodb",{"password":items["vietpulse-mongodb-password"]["password"]}),
                  ("processing-es",{"password":items["vietpulse-es-es-elastic-user"]["elastic"],
                                    "ca.crt":items["vietpulse-es-es-http-certs-public"]["tls.crt"]})):
    result.append({"apiVersion":"v1","kind":"Secret","metadata":{"name":name,"namespace":"processing",
                   "labels":{"vietpulse.io/managed":"processing"}},"type":"Opaque","data":data})
print(json.dumps({"apiVersion":"v1","kind":"List","items":result}))
' | k -n processing apply -f -
REMOTE
printf 'PASS: processing credential copies updated without printing their values.\n'
