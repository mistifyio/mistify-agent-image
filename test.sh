#/usr/bin/env bash
set -e
set -o pipefail
set -x

ADDRESS=${1:-127.0.0.1:16000}

request (){
    METHOD=$1
    shift
    ID=$RANDOM
    PARAMS="$@"

    DATA=$(printf '{ "id": %d, "method": "ImageStore.%s", "params": [%s] }' $ID $METHOD "$PARAMS")
    
    curl --fail -sv -X POST -H 'Content-Type: application/json' http://$ADDRESS/_mistify_RPC_ -d "$DATA" | jq .
}

request ListVolumes

request CreateVolume '{"id": "guests/foo", "size": 8 }'

request ListVolumes

request DeleteDataset '{"id": "guests/foo" }'

request ListVolumes

request RequestImage '{ "source": "http://omnios.omniti.com/nothere/ubuntu-14.04-server-mistify-amd64-disk1.zfs.gz" }'

request ListImages


