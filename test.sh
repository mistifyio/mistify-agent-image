#/usr/bin/env bash
set -e
set -o pipefail
set -x

if [ $(uname) != "Linux" ]; then
    echo "you probably meant to run this in side a dev vm"
    exit -1
fi



pushd cmd/mistify-agent-image
go build
sudo ./mistify-agent-image &
AGENT_PID=$!
trap "sudo kill $AGENT_PID" SIGINT SIGTERM EXIT
popd

sleep 1

request (){
    METHOD=$1
    shift
    ID=$RANDOM
    PARAMS="$@"

    DATA=$(printf '{ "id": %d, "method": "ImageStore.%s", "params": [%s] }' $ID $METHOD "$PARAMS")

    curl --fail -sv -X POST -H 'Content-Type: application/json' http://127.0.0.1:19999/_mistify_RPC_ -d "$DATA" | jq .
}

request ListVolumes

request CreateVolume '{"id": "guests/foo", "size": 8 }'

request ListVolumes

request DeleteDataset '{"id": "guests/foo" }'

request ListVolumes

request RequestImage '{ "source": "http://omnios.omniti.com/nothere/ubuntu-14.04-server-mistify-amd64-disk1.zfs.gz" }'

request ListImages


