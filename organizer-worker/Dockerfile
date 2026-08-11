FROM python:3.11-slim

WORKDIR /app

ARG SERVICE_NAME=organizer-worker
ARG BUILD_GIT_SHA=unknown
ARG BUILD_TIMESTAMP_UTC=unknown
ARG BUILD_SOURCE_MARKER=
ARG BUILD_ROUTE_RULES_VERSION=
ARG BUILD_HANDLER_SHA256=
ARG BUILD_DOCKERFILE_PATH=./organizer-worker/Dockerfile
ARG BUILD_CONTEXT_PATH=.
ARG BUILD_ENTRYPOINT_MODULE=worker-runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends sqlite3 \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    requests==2.32.3 \
    pyyaml \
    google-auth \
    google-auth-oauthlib \
    google-api-python-client \
    googleapis-common-protos \
    httplib2

COPY organizer-worker/worker.py /app/worker.py
COPY organizer-worker/shared_runtime.py /app/shared_runtime.py
COPY organizer-worker/runtime_handlers.py /app/runtime_handlers.py
COPY organizer-worker/runtime_calendar.py /app/runtime_calendar.py
COPY organizer-worker/runtime_search.py /app/runtime_search.py
COPY organizer-worker/runtime_server.py /app/runtime_server.py
COPY organizer-worker/runtime_sync_conflicts.py /app/runtime_sync_conflicts.py
COPY organizer-worker/legacy_queue.py /app/legacy_queue.py
COPY common /app/common
COPY organizer-worker/src /app/src
COPY organizer-worker/entrypoint.sh /app/entrypoint.sh
RUN python -c "import json,pathlib; pathlib.Path('/app/build-info.json').write_text(json.dumps({'service':'${SERVICE_NAME}','git_sha':'${BUILD_GIT_SHA}','build_timestamp_utc':'${BUILD_TIMESTAMP_UTC}','route_rules_version':'${BUILD_ROUTE_RULES_VERSION}','source_marker':'${BUILD_SOURCE_MARKER}','handler_sha256':'${BUILD_HANDLER_SHA256}','dockerfile_path':'${BUILD_DOCKERFILE_PATH}','build_context':'${BUILD_CONTEXT_PATH}','entrypoint_module':'${BUILD_ENTRYPOINT_MODULE}'}, ensure_ascii=False, indent=2), encoding='utf-8')"
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
