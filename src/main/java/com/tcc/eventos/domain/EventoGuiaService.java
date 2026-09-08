package com.tcc.eventos.domain;

import com.tcc.eventos.api.EventoGuiaRequest;
import com.tcc.eventos.api.EventoGuiaResponse;
import org.springframework.stereotype.Service;

import java.time.Instant;

/**
 * Orquesta la regla de negocio (idempotencia + publicación) sin conocer
 * CÓMO se implementa cada una — solo conoce las interfaces IdempotencyChecker
 * y EventoPublisher. Esto es Dependency Inversion aplicado: el módulo de
 * alto nivel (esta clase, que contiene la lógica de negocio) no depende de
 * detalles de infraestructura (memoria, Kafka), depende de abstracciones.
 */
@Service
public class EventoGuiaService {

    private final IdempotencyChecker idempotencyChecker;
    private final EventoPublisher publisher;

    public EventoGuiaService(IdempotencyChecker idempotencyChecker, EventoPublisher publisher) {
        this.idempotencyChecker = idempotencyChecker;
        this.publisher = publisher;
    }

    public EventoGuiaResponse procesar(EventoGuiaRequest request) {

        boolean esDuplicado = idempotencyChecker.marcarComoProcesadoSiEsNuevo(request.eventoId());

        if (esDuplicado) {
            // No se vuelve a publicar: el cliente puede reintentar con seguridad
            // (ej. tras un timeout) sin generar eventos repetidos aguas abajo.
            return new EventoGuiaResponse(
                    request.eventoId(),
                    request.numeroGuia(),
                    "DUPLICADO",
                    "Este evento ya había sido recibido; no se publica de nuevo."
            );
        }

        EventoGuia evento = new EventoGuia(
                request.eventoId(),
                request.numeroGuia(),
                request.estado(),
                request.ocurridoEn(),
                Instant.now(),
                request.origen(),
                request.metadata()
        );

        publisher.publicar(evento);

        return new EventoGuiaResponse(
                request.eventoId(),
                request.numeroGuia(),
                "ACEPTADO",
                "Evento aceptado y publicado."
        );
    }
}
