#!/bin/bash
set -o nounset
set -o errexit

network_name=cloud-tasks-emulator-net
test_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "Running smoketests for docker image $EMULATOR_DOCKER_IMAGE"

echo "Creating docker network $network_name"
docker network create "$network_name"

echo "Starting emulator"
# Intentionally uses a non-standard port to verify that the port argument is handled as expected
docker run \
  --rm \
  -d \
  -p 8930:8930 \
  -p 8050:8050 \
  --name cloud-tasks-emulator \
  --network $network_name \
  "$EMULATOR_DOCKER_IMAGE" \
  -host 0.0.0.0 \
  -queue projects/test-project/locations/us-central1/queues/test \
  -port 8930 \
  -openid-issuer http://cloud-tasks-emulator:8050

echo ""
echo "-------------------"
echo "Running smoketests"
set +o errexit
docker run \
  --rm \
  -v $test_dir:/go/src \
  -v /tmp/smoketest-packages:/go/pkg \
  -w /go/src \
  -p 8920:8920 \
  --name ct-smoketests \
  --network $network_name \
  golang:1.13-alpine \
  go run smoketests.go \
  -emulator-port 8930

test_result=$?
set -o errexit

echo "Test completed with code $test_result"
echo ""
echo "-------------------"
echo "Logs from emulator:"
docker logs cloud-tasks-emulator

echo ""
echo "-------------------"
echo "Cleaning up"
docker kill cloud-tasks-emulator
docker network rm "$network_name"

exit $test_result
