package com.tcc.eventos.api;

import com.tcc.eventos.domain.EventoGuiaService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/eventos-guia")
public class EventoGuiaController {

    private final EventoGuiaService service;

    public EventoGuiaController(EventoGuiaService service) {
        this.service = service;
    }

    /**
     * Recibe un evento de estado de una guía y lo publica al flujo de eventos.
     * Idempotente en eventoId: reintentar la misma petición es seguro.
     */
    @PostMapping
    public ResponseEntity<EventoGuiaResponse> recibirEvento(@Valid @RequestBody EventoGuiaRequest request) {
        EventoGuiaResponse response = service.procesar(request);

        HttpStatus status = "DUPLICADO".equals(response.estadoProcesamiento())
                ? HttpStatus.OK
                : HttpStatus.ACCEPTED; // 202: aceptado para procesamiento asíncrono

        return ResponseEntity.status(status).body(response);
    }
}
