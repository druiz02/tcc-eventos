package com.tcc.eventos.domain;

/**
 * Puerto que el dominio usa para publicar un evento, sin conocer el
 * mecanismo de transporte concreto (Kafka, un outbox transaccional,
 * otra plataforma de mensajería, etc.).
 *
 * Esto es lo que permite, por ejemplo, migrar de "publicar directo a
 * Kafka" a "insertar en una tabla outbox y dejar que un proceso aparte
 * publique" sin modificar EventoGuiaService en absoluto — solo cambia
 * la implementación concreta que Spring inyecta.
 */
public interface EventoPublisher {

    void publicar(EventoGuia evento);
}
