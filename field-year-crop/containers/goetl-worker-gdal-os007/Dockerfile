FROM golang:1.26.2-bookworm AS build

WORKDIR /src

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        g++ \
        gdal-bin \
        libgdal-dev \
        make \
        python3 \
    && rm -rf /var/lib/apt/lists/*

COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal

RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 \
    go build -tags gdal -trimpath -o /out/goetl-worker ./cmd/worker && \
    go build -tags gdal -trimpath -o /out/goet-geospatial ./cmd/goet-geospatial

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        gdal-bin \
        p7zip-full \
        python3 \
        python3-gdal \
        python3-numpy \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /goetl
ENV PATH="/goetl:${PATH}"

COPY --from=build /out/goetl-worker /goetl/goetl-worker
COPY --from=build /out/goet-geospatial /goetl/goet-geospatial

RUN ln -s /goetl/goetl-worker /usr/local/bin/goetl-worker \
    && ln -s /goetl/goet-geospatial /usr/local/bin/goet-geospatial

ENTRYPOINT ["/goetl/goetl-worker"]
