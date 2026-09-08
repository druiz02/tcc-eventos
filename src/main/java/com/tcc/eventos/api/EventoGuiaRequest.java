package com.tcc.eventos.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Payload que envía cualquier sistema origen (TMS, app del mensajero,
 * integraciones con transportadoras, etc.) cuando cambia el estado de una guía.
 *
 * eventoId es generado por el CLIENTE (o por el sistema origen) y es la clave
 * de idempotencia: si el mismo evento se reintenta o llega duplicado por una
 * caída de red, el servicio debe reconocerlo y no publicarlo dos veces.
 */
public record EventoGuiaRequest(

        @NotNull
        UUID eventoId,

        @NotBlank
        String numeroGuia,

        @NotBlank
        String estado, // ej: EN_TRANSITO, EN_REPARTO, ENTREGADO, NOVEDAD, DEVUELTO

        @NotNull
        Instant ocurridoEn, // timestamp de negocio (cuándo pasó), no de ingesta

        String origen, // sistema o dispositivo que reporta el evento

        Map<String, Object> metadata // datos libres: geolocalización, causal de novedad, etc.
) {
}
