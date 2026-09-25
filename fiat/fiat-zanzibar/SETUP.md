## SpiceDB

`brew install authzed/tap/spicedb authzed/tap/zed`

`createdb -h localhost -p 5432 -U postgres spicedb`

```
spicedb migrate head \
  --datastore-engine=postgres \
  --datastore-conn-uri="postgres://postgres@localhost:5432/spicedb?sslmode=disable"
```

```
spicedb serve \
  --datastore-engine=postgres \
  --datastore-conn-uri="postgres://postgres@localhost:5432/spicedb?sslmode=disable" \
  --grpc-preshared-key="local-secret-key" \
  --http-enabled
```

## OpenFGA

`brew install openfga`

`createdb -h localhost -p 5432 -U postgres openfga`

```
openfga migrate \
  --datastore-engine postgres \
  --datastore-uri "postgres://postgres@localhost:5432/openfga?sslmode=disable"
```

```
openfga run \
  --datastore-engine postgres \
  --datastore-uri "postgres://postgres@localhost:5432/openfga?sslmode=disable" \
  --http-addr "0.0.0.0:9190" \
  --grpc-addr "0.0.0.0:9191" \
  --playground-addr "0.0.0.0:3005" \
  --metrics-addr "0.0.0.0:2222"
```

## Load Tests

```
./gradlew :kork:kork-zanzibar:test --tests "*ZanzibarStoreBenchmark" \
    -Dzanzibar.benchmark=true \
    -Dzanzibar.bench.users=2000 \
    -Dzanzibar.bench.rolesPerUser=10000 -Dzanzibar.bench.resourcesPerType=3000
```
