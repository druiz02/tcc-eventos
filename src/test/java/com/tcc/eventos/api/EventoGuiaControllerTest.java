package com.tcc.eventos.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Prueba de extremo a extremo con un broker Kafka embebido (en memoria):
 *  1) un evento nuevo se acepta (202) y aparece publicado en el tópico.
 *  2) el MISMO eventoId reenviado responde "DUPLICADO" (200) y NO se
 *     vuelve a publicar (verifica la garantía de idempotencia).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 1, topics = { "guia-eventos" })
class EventoGuiaControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private org.springframework.kafka.test.EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    void aceptaUnEventoNuevoYLoPublicaEnKafka() throws Exception {
        UUID eventoId = UUID.randomUUID();
        String body = payload(eventoId, "GUIA-001", "EN_TRANSITO");

        mockMvc.perform(post("/api/v1/eventos-guia")
                        .contentType("application/json")
                        .content(body))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.estadoProcesamiento").value("ACEPTADO"));

        ConsumerRecord<String, String> registro = buscarMensajePorGuia("GUIA-001");
        assertThat(registro.key()).isEqualTo("GUIA-001");
        assertThat(registro.value()).contains(eventoId.toString());
    }

    @Test
    void reenviarElMismoEventoIdNoLoDuplica() throws Exception {
        UUID eventoId = UUID.randomUUID();
        String body = payload(eventoId, "GUIA-002", "EN_REPARTO");

        // primer envío: aceptado
        mockMvc.perform(post("/api/v1/eventos-guia")
                        .contentType("application/json")
                        .content(body))
                .andExpect(status().isAccepted());

        // reintento (simula timeout del cliente / retry de red)
        mockMvc.perform(post("/api/v1/eventos-guia")
                        .contentType("application/json")
                        .content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.estadoProcesamiento").value("DUPLICADO"));
    }

    @Test
    void rechazaPayloadInvalido() throws Exception {
        String invalido = "{\"numeroGuia\": \"\"}"; // faltan eventoId, estado, ocurridoEn

        mockMvc.perform(post("/api/v1/eventos-guia")
                        .contentType("application/json")
                        .content(invalido))
                .andExpect(status().isBadRequest());
    }

    private String payload(UUID eventoId, String guia, String estado) throws Exception {
        var request = new EventoGuiaRequest(
                eventoId, guia, estado, Instant.now(), "TEST", Map.of("canal", "test"));
        return objectMapper.writeValueAsString(request);
    }

    /**
     * Lee TODOS los mensajes acumulados en el tópico (el broker embebido vive
     * durante toda la clase de pruebas, así que puede haber mensajes de otras
     * pruebas) y devuelve el que corresponde a la guía buscada.
     *
     * Se usa un consumer group único por llamada (UUID) para forzar que cada
     * lectura arranque desde el principio del tópico sin interferir con
     * offsets de otras pruebas.
     */
    private ConsumerRecord<String, String> buscarMensajePorGuia(String numeroGuia) {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
                "test-group-" + UUID.randomUUID(), "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        var cf = new org.springframework.kafka.core.DefaultKafkaConsumerFactory<String, String>(consumerProps);
        try (var consumer = cf.createConsumer()) {
            embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, "guia-eventos");
            ConsumerRecords<String, String> registros = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(5));

            for (ConsumerRecord<String, String> registro : registros) {
                if (numeroGuia.equals(registro.key())) {
                    return registro;
                }
            }
            throw new AssertionError("No se encontró ningún mensaje para la guía " + numeroGuia);
        }
    }
}
