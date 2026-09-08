package com.tcc.eventos.api;

import java.util.UUID;

public record EventoGuiaResponse(
        UUID eventoId,
        String numeroGuia,
        String estadoProcesamiento, // ACEPTADO | DUPLICADO
        String mensaje
) {
}
