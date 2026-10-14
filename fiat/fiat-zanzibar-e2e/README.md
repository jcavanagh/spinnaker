# Zanzibar end-to-end tests

Runs store-backed Fiat with the services that write to it and check it:

- containers: SpiceDB, MySQL, Redis
- processes, from each service's `installDist`: Fiat (two replicas), Front50, Clouddriver, Gate,
  Orca, Igor, Echo

Users log in through Gate header authentication. Fiat reads their roles from a file the tests
rewrite. Service configuration is in `src/test/resources/config`.

## Run

Requires Docker and JDK 25. From the repository root, build the distributions, then run the tests:

```
./gradlew :clouddriver:clouddriver-web:installDist :echo:echo-web:installDist :front50:front50-web:installDist :gate:gate-web:installDist :igor:igor-web:installDist :orca:orca-web:installDist
./gradlew :fiat:fiat-zanzibar-e2e:test -Dzanzibar.e2e=true
```

The test task builds Fiat's distribution itself. Rebuild the others after changing them.

## Output

In `fiat/fiat-zanzibar-e2e/build/zanzibar-e2e`:

- `logs/<service>.log`: each service's output
- `propagation.md`: how long each change took to reach each service
