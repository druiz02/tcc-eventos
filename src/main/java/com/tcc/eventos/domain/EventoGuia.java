package com.tcc.eventos.domain;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/** Evento de dominio que se serializa (JSON) y se publica en Kafka. */
public record EventoGuia(
        UUID eventoId,
        String numeroGuia,
        String estado,
        Instant ocurridoEn,
        Instant recibidoEn,
        String origen,
        Map<String, Object> metadata
) {
}
