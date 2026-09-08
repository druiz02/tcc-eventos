package com.tcc.eventos.infra.idempotency;

import com.tcc.eventos.domain.IdempotencyChecker;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementación concreta de IdempotencyChecker: guarda qué eventoId ya
 * fue procesado para poder responder "duplicado" sin volver a publicar.
 *
 * IMPORTANTE (para la sustentación): esta implementación es in-memory
 * y sirve solo para el ejercicio / pruebas locales. En producción esto
 * NO puede vivir en memoria de un solo pod porque:
 *   1) se pierde si el pod se reinicia,
 *   2) no es compartido entre réplicas detrás del balanceador.
 *
 * La versión real usaría:
 *   - Una tabla `eventos_procesados(evento_id PK, numero_guia, procesado_en)`
 *     en la misma transacción de escritura del outbox (patrón Transactional
 *     Outbox), o
 *   - Redis con TTL (SETNX) si se acepta idempotencia "best effort" con
 *     ventana de tiempo en vez de histórico completo.
 *
 * Gracias a que EventoGuiaService depende de la interfaz IdempotencyChecker
 * y no de esta clase, cualquiera de esas migraciones se hace escribiendo
 * una nueva implementación (ej. RedisIdempotencyChecker), sin modificar
 * ni una línea de la lógica de negocio.
 */
@Component
public class InMemoryIdempotencyChecker implements IdempotencyChecker {

    private final Map<UUID, Instant> procesados = new ConcurrentHashMap<>();
    private static final Duration TTL = Duration.ofHours(24);

    @Override
    public boolean marcarComoProcesadoSiEsNuevo(UUID eventoId) {
        Instant ahora = Instant.now();
        Instant previo = procesados.putIfAbsent(eventoId, ahora);
        return previo != null;
    }

    // Nota: en esta implementación de ejercicio no hay purga por TTL activa;
    // en Redis esto sería automático con EXPIRE.
}
