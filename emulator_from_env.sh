#!/bin/sh

BINARY="/emulator"

set --

if [ -n "$HOST" ]; then
  set -- "$@" -host="$HOST"
fi

if [ -n "$PORT" ]; then
  set -- "$@" -port="$PORT"
fi

if [ -n "$HARD_RESET_ON_PURGE_QUEUE" ]; then
  set -- "$@" -hard-reset-on-purge-queue="$HARD_RESET_ON_PURGE_QUEUE"
fi

if [ -n "$OPENID_ISSUER" ]; then
  set -- "$@" -openid-issuer="$OPENID_ISSUER"
fi

if [ -n "$INITIAL_QUEUES" ]; then
  old_ifs=$IFS
  IFS=","
  for q in $INITIAL_QUEUES; do
    set -- "$@" -queue="$q"
  done
  IFS=$old_ifs
fi

exec "$BINARY" "$@"
