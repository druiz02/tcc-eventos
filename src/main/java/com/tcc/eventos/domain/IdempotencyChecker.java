package com.tcc.eventos.domain;

import java.util.UUID;

/**
 * Puerto (en el sentido de arquitectura hexagonal) que el dominio usa para
 * saber si un evento ya fue procesado, sin conocer CÓMO se almacena esa
 * información.
 *
 * El dominio (EventoGuiaService) depende de esta abstracción, nunca de una
 * implementación concreta (memoria, Redis, base de datos). Esto es lo que
 * permite cambiar la estrategia de idempotencia sin tocar la lógica de
 * negocio — por ejemplo, migrar de un mapa en memoria a Redis compartido
 * entre réplicas, que es justo lo que haría falta antes de producción.
 */
public interface IdempotencyChecker {

    /**
     * Marca el eventoId como procesado si es la primera vez que se ve.
     *
     * @return true si el evento YA existía (es decir, es un duplicado);
     *         false si es la primera vez que se procesa.
     */
    boolean marcarComoProcesadoSiEsNuevo(UUID eventoId);
}
