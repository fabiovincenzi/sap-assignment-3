# Shipping on the Air

Drone-based package delivery system: three microservices behind an API gateway.

Assignment 3 starts from the Assignment 2 solution
([sap-assignment-2](https://github.com/fabiovincenzi/sap-assignment-2)) and refines it with an
event-driven architecture based on Apache Kafka, measured service levels and a Kubernetes
deployment.

## Build

```
./gradlew installDist
```

## Run with Docker

```
docker compose up -d
```

Entry point: the API gateway on http://localhost:8080/api/v1. Prometheus on http://localhost:9090.

## Run locally

Open 4 terminals and execute:

```
./order-service/build/install/order-service/bin/order-service
./delivery-service/build/install/delivery-service/bin/delivery-service
./drone-service/build/install/drone-service/bin/drone-service
./api-gateway/build/install/api-gateway/bin/api-gateway
```

Ports: Order 8090, Delivery 8091, Drone 8092, Gateway 8080.

## API Documentation (Swagger UI)

The services that kept their REST API expose the OpenAPI spec and an interactive Swagger UI.
The delivery service does not: it is reached over event channels and serves only its health check.

| Service          | Swagger UI                           | OpenAPI Spec                           |
|------------------|--------------------------------------|----------------------------------------|
| Order Service    | http://localhost:8090/swagger-ui     | http://localhost:8090/api/openapi.yaml |
| Drone Service    | http://localhost:8092/swagger-ui     | http://localhost:8092/api/openapi.yaml |

## Test

```
./gradlew test
```

Runs the unit, integration and component tests, plus the ArchUnit fitness functions.

### End-to-end test

It drives the whole system through the API gateway, so the system must be up first. It is kept
out of the normal build and only runs when the `e2e` property is given:

```
docker compose up -d
./gradlew :system-end-to-end:test -Pe2e
```
