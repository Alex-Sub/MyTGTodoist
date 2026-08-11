FROM python:3.11-slim

WORKDIR /app

ARG SERVICE_NAME=telegram-bot
ARG BUILD_GIT_SHA=unknown
ARG BUILD_TIMESTAMP_UTC=unknown
ARG BUILD_SOURCE_MARKER=
ARG BUILD_ROUTE_RULES_VERSION=
ARG BUILD_HANDLER_SHA256=
ARG BUILD_DOCKERFILE_PATH=./telegram-bot/Dockerfile
ARG BUILD_CONTEXT_PATH=.
ARG BUILD_ENTRYPOINT_MODULE=src.integrations.telegram.bot_main

RUN pip install --no-cache-dir requests==2.32.3 PyYAML==6.0.2

ENV PYTHONPATH=/app:/app/app-integration

COPY telegram-bot/app-integration /app/app-integration
COPY common /app/common

RUN python -c "import json,pathlib; pathlib.Path('/app/build-info.json').write_text(json.dumps({'service':'${SERVICE_NAME}','git_sha':'${BUILD_GIT_SHA}','build_timestamp_utc':'${BUILD_TIMESTAMP_UTC}','route_rules_version':'${BUILD_ROUTE_RULES_VERSION}','source_marker':'${BUILD_SOURCE_MARKER}','handler_sha256':'${BUILD_HANDLER_SHA256}','dockerfile_path':'${BUILD_DOCKERFILE_PATH}','build_context':'${BUILD_CONTEXT_PATH}','entrypoint_module':'${BUILD_ENTRYPOINT_MODULE}'}, ensure_ascii=False, indent=2), encoding='utf-8')"

CMD ["python", "-m", "src.integrations.telegram.bot_main"]
