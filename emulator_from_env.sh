#!/bin/sh

RUN_CMD="/emulator"

if [ -n "$HOST" ]; then
  RUN_CMD="$RUN_CMD -host=$HOST"
fi

if [ -n "$PORT" ]; then
  RUN_CMD="$RUN_CMD -port=$PORT"
fi

if [ -n "$HARD_RESET_ON_PURGE_QUEUE" ]; then
  RUN_CMD="$RUN_CMD -hard_reset_on_purge_queue=$HARD_RESET_ON_PURGE_QUEUE"
fi

if [ -n "$OPENID_ISSUER" ]; then
  RUN_CMD="$RUN_CMD -openid_issuer=$OPENID_ISSUER"
fi

if [ -n "$INITIAL_QUEUES" ]; then
  IFS=","
  set -- $INITIAL_QUEUES
  for i in "$@"; do
    RUN_CMD="$RUN_CMD -queue=$i"
  done
fi

`$RUN_CMD`
