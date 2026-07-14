FROM alpine:3.22 AS runtime
ARG TARGETARCH
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY dist/${TARGETARCH}/ushio-runtime /usr/local/bin/ushio-runtime
CMD ["ushio-runtime"]
