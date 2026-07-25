FROM alpine:3.22 AS runtime
ARG TARGETARCH
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY dist/${TARGETARCH}/nike-runtime /usr/local/bin/nike-runtime
CMD ["nike-runtime"]
