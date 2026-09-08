# tcc-eventos-guia

Servicio REST que recibe un evento de estado de una guía (TMS, app del
mensajero, transportadoras aliadas, etc.) y lo publica a un tópico de Kafka
(`guia-eventos`) para que otros sistemas (actualización de estado,
notificaciones al cliente, tracking/analítica) lo consuman de forma
desacoplada y a su propio ritmo.

Es el **Ejercicio Previo** de la prueba técnica Advance — sirve como base de
conversación para el Eje 1 (arquitectura) y el Eje 2 (código/calidad).

## Cómo correrlo

Requiere Java 17+ y Maven. No necesitas un Kafka real para correr las
pruebas: usan un broker embebido en memoria (`spring-kafka-test`).

```bash
mvn test          # corre las 3 pruebas (evento nuevo, duplicado, payload inválido)
mvn spring-boot:run   # para levantarlo de verdad necesitas un Kafka en localhost:9092
```

## Endpoint

```
POST /api/v1/eventos-guia
Content-Type: application/json

{
  "eventoId": "b3b1e2b0-...-uuid",
  "numeroGuia": "TCC-000123456",
  "estado": "EN_REPARTO",
  "ocurridoEn": "2026-09-04T14:32:00Z",
  "origen": "APP_MENSAJERO",
  "metadata": { "lat": 6.25, "lng": -75.56 }
}
```

Respuestas:
- `202 Accepted` — evento nuevo, aceptado y publicado.
- `200 OK` con `estadoProcesamiento: "DUPLICADO"` — el `eventoId` ya se había
  procesado; no se publica de nuevo (idempotencia).
- `400 Bad Request` — payload inválido.

## Decisiones de diseño (y sus trade-offs)

**1. Idempotencia por `eventoId` generado por el emisor, no por el servidor.**
El sistema origen (o el dispositivo del mensajero) genera un UUID por evento.
Así, si una petición se reintenta por timeout de red, el servidor puede
reconocer que es el mismo evento sin ambigüedad.
*Trade-off*: exige disciplina en los sistemas origen para no reusar IDs ni
generar uno nuevo en cada reintento (si reintentan con un ID nuevo, la
idempotencia no protege). Se documenta como contrato del API.

**2. `numeroGuia` como key de partición en Kafka.**
Garantiza orden estricto de eventos *de una misma guía* (útil porque el
consumidor que actualiza el estado necesita ver "EN_TRANSITO" antes que
"ENTREGADO"), mientras distintas guías se procesan en paralelo en distintas
particiones → esto es lo que da la alta concurrencia.
*Trade-off*: si una guía específica genera muchísimos eventos (guía "caliente"),
esa partición puede volverse un cuello de botella; en el diseño completo esto
se mitiga con suficientes particiones y monitoreo de "hot partitions".

**3. Interfaces para idempotencia y publicación (Dependency Inversion).**
`EventoGuiaService` depende de las abstracciones `IdempotencyChecker` y
`EventoPublisher`, no de las clases concretas (`InMemoryIdempotencyChecker`,
`KafkaEventoPublisher`). Esto es lo que permite migrar la idempotencia a
Redis o el publisher a un outbox transaccional escribiendo una nueva
implementación, sin tocar la lógica de negocio.

**4. Almacén de idempotencia in-memory en este ejercicio.**
Se deja explícito en el código (`InMemoryIdempotencyChecker`) que esto es
solo para el ejercicio. En producción sería una tabla `eventos_procesados`
en la misma transacción de escritura (patrón **Transactional Outbox**) o
Redis con TTL, para que sea consistente entre réplicas del servicio.

**5. `acks=all` + idempotencia de productor de Kafka + reintentos infinitos
en el productor.**
Prioriza "no perder eventos" sobre latencia mínima. Es la decisión correcta
para eventos de negocio (estado de una guía), no para telemetría descartable.

**6. `202 Accepted` en vez de `200/201`.**
El servicio confirma que el evento fue *aceptado para procesamiento*, no que
ya impactó el estado final visible al cliente (eso lo hace el consumidor,
de forma asíncrona). Es una señal honesta del contrato del API.

## Qué falta para producción (fuera de alcance del ejercicio, para discutir en el Eje 1)

- Autenticación/autorización (OAuth2 client-credentials entre sistemas internos).
- Outbox transaccional real respaldado por base de datos (la interfaz
  `EventoPublisher` ya deja ese cambio listo para implementarse sin tocar
  el dominio).
- Tópico y flujo de reintentos con dead-letter topic para eventos "poison".
- Trazas distribuidas (correlación `eventoId` end-to-end) y dashboards de lag
  de consumidores.
- Pipeline CI/CD (build, pruebas, análisis estático, imagen de contenedor,
  despliegue canario a Kubernetes).
