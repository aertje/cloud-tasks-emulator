#!/bin/bash
set -o nounset
set -o errexit

network_name=cloud-tasks-emulator-net
test_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

overall_result=0

cleanup() {
  echo ""
  echo "-------------------"
  echo "Cleaning up"
  docker rm -f cloud-tasks-emulator >/dev/null 2>&1 || true
  docker rm -f cloud-tasks-emulator-env >/dev/null 2>&1 || true
  docker rm -f ct-smoketests >/dev/null 2>&1 || true
  docker rm -f ct-smoketests-env >/dev/null 2>&1 || true
  docker network rm "$network_name" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "Running smoketests for docker image $EMULATOR_DOCKER_IMAGE"

echo "Creating docker network $network_name"
docker network create "$network_name" 2>/dev/null || true

echo ""
echo "-------------------"
echo "Starting emulator (default entrypoint)"
# Intentionally uses a non-standard port to verify that the port argument is handled as expected
docker run \
  -d \
  --name cloud-tasks-emulator \
  --network "$network_name" \
  "$EMULATOR_DOCKER_IMAGE" \
  -host 0.0.0.0 \
  -queue projects/test-project/locations/us-central1/queues/test \
  -port 8930 \
  -openid-issuer http://cloud-tasks-emulator:8050

echo ""
echo "-------------------"
echo "Running smoketests (phase 1, default entrypoint)"
set +o errexit
docker run \
  --rm \
  -v "$test_dir":/go/src \
  -v /tmp/smoketest-packages:/go/pkg \
  -w /go/src \
  --name ct-smoketests \
  --network "$network_name" \
  golang:1.26-alpine \
  go run smoketests.go \
  -emulator-port 8930

test_result=$?
set -o errexit

echo "Phase 1 smoketests completed with code $test_result"
if [ "$test_result" -ne 0 ]; then
  overall_result=1
fi

echo ""
echo "-------------------"
echo "Logs from emulator:"
docker logs cloud-tasks-emulator

echo ""
echo "-------------------"
echo "Stopping emulator and checking for graceful shutdown"
set +o errexit
docker stop --time 10 cloud-tasks-emulator
set -o errexit
emulator_exit_code=$(docker inspect -f '{{.State.ExitCode}}' cloud-tasks-emulator)
echo "Emulator exit code: $emulator_exit_code"
if [ "$emulator_exit_code" != "0" ]; then
  echo "FAILURE: emulator did not shut down gracefully (exit code $emulator_exit_code; 137 indicates it was killed with SIGKILL rather than stopping on SIGTERM)"
  overall_result=1
fi

if [ "$test_result" -ne 0 ]; then
  echo ""
  echo "-------------------"
  echo "Skipping phase 2 (env-var entrypoint) because phase 1 smoketests failed"
else
  echo ""
  echo "-------------------"
  echo "Starting emulator (env-var entrypoint emulator_from_env.sh, comma-separated INITIAL_QUEUES)"
  docker run \
    -d \
    --name cloud-tasks-emulator-env \
    --network "$network_name" \
    --entrypoint ./emulator_from_env.sh \
    -e HOST=0.0.0.0 \
    -e PORT=8931 \
    -e OPENID_ISSUER=http://cloud-tasks-emulator-env:8050 \
    -e INITIAL_QUEUES=projects/test-project/locations/us-central1/queues/queue-a,projects/test-project/locations/us-central1/queues/queue-b \
    "$EMULATOR_DOCKER_IMAGE"

  echo ""
  echo "-------------------"
  echo "Running smoketests (phase 2, env-var entrypoint)"
  set +o errexit
  docker run \
    --rm \
    -v "$test_dir":/go/src \
    -v /tmp/smoketest-packages:/go/pkg \
    -w /go/src \
    --name ct-smoketests-env \
    --network "$network_name" \
    golang:1.26-alpine \
    go run smoketests.go \
    -emulator-host cloud-tasks-emulator-env \
    -emulator-port 8931 \
    -http-handler-host ct-smoketests-env \
    -queue-path projects/test-project/locations/us-central1/queues/queue-b \
    -expect-queue projects/test-project/locations/us-central1/queues/queue-a

  test_result_env=$?
  set -o errexit

  echo "Phase 2 smoketests completed with code $test_result_env"
  if [ "$test_result_env" -ne 0 ]; then
    overall_result=1
  fi

  echo ""
  echo "-------------------"
  echo "Logs from emulator-env:"
  docker logs cloud-tasks-emulator-env

  echo ""
  echo "-------------------"
  echo "Stopping emulator-env and checking for graceful shutdown"
  set +o errexit
  docker stop --time 10 cloud-tasks-emulator-env
  set -o errexit
  emulator_env_exit_code=$(docker inspect -f '{{.State.ExitCode}}' cloud-tasks-emulator-env)
  echo "Emulator-env exit code: $emulator_env_exit_code"
  if [ "$emulator_env_exit_code" != "0" ]; then
    echo "FAILURE: emulator-env did not shut down gracefully (exit code $emulator_env_exit_code; 137 indicates it was killed with SIGKILL rather than stopping on SIGTERM)"
    overall_result=1
  fi
fi

echo ""
echo "-------------------"
echo "Summary"
if [ "$overall_result" -eq 0 ]; then
  echo "All smoketests passed"
else
  echo "Smoketests FAILED, see above for details"
fi

exit $overall_result
