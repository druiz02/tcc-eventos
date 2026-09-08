package com.tcc.eventos.infra.kafka;

import com.tcc.eventos.domain.EventoGuia;
import com.tcc.eventos.domain.EventoPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

/**
 * Implementación concreta de EventoPublisher que publica en Kafka, en el
 * tópico "guia-eventos".
 *
 * Decisiones clave:
 *  - Se usa numeroGuia como KEY del mensaje -> Kafka garantiza orden dentro
 *    de la misma partición para una misma guía (los eventos de una guía
 *    nunca se desordenan entre sí, aunque distintas guías se procesen en
 *    paralelo en distintas particiones). Esto es lo que da la concurrencia alta
 *    sin sacrificar consistencia por guía.
 *  - El header "eventoId" viaja explícito para que los consumidores puedan
 *    deduplicar también del lado de consumo (defensa en profundidad).
 *
 * EventoGuiaService no conoce esta clase directamente, solo la interfaz
 * EventoPublisher — así, migrar a un outbox transaccional (donde en vez de
 * publicar directo a Kafka se inserta en una tabla) es escribir una nueva
 * implementación de EventoPublisher, sin tocar el dominio.
 */
@Component
public class KafkaEventoPublisher implements EventoPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventoPublisher.class);
    public static final String TOPIC = "guia-eventos";

    private final KafkaTemplate<String, EventoGuia> kafkaTemplate;

    public KafkaEventoPublisher(KafkaTemplate<String, EventoGuia> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void publicar(EventoGuia evento) {
        var mensaje = MessageBuilder
                .withPayload(evento)
                .setHeader(KafkaHeaders.TOPIC, TOPIC)
                .setHeader(KafkaHeaders.KEY, evento.numeroGuia())
                .setHeader("eventoId", evento.eventoId().toString())
                .build();

        kafkaTemplate.send(mensaje).whenComplete((result, ex) -> {
            if (ex != null) {
                // El productor de Spring Kafka ya reintenta internamente (retries,
                // backoff) según configuración; si aun así falla, se loguea con
                // el eventoId para poder correlacionar y, si aplica, reprocesar
                // desde la tabla outbox.
                log.error("Fallo publicando eventoId={} guia={}",
                        evento.eventoId(), evento.numeroGuia(), ex);
            } else {
                log.info("Evento publicado eventoId={} guia={} partition={} offset={}",
                        evento.eventoId(), evento.numeroGuia(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
}
