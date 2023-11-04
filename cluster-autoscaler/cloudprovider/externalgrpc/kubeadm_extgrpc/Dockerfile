FROM ubuntu:22.04

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      && rm -rf /var/lib/apt/lists/*

ENV ADDRESS="127.0.0.1:8086"
ENV API_ADDRESS="192.168.105.19:6443"
ENV CERT_HASH="sha256:58c7f2c68db3c04b8b5932c201e057623a35be1904904449d90d484f15f4e37e"
ENV TOKEN="y8y9fz.ah2ng7uq8qu4ewtr"
ENV LOG_LEVEL="1"

COPY ca-ext-grpc-kubeadm-cloud-provider-arm64 /usr/local/bin/

