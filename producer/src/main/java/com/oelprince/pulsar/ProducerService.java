package com.oelprince.pulsar;

import java.util.logging.Logger;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.eclipse.microprofile.config.inject.ConfigProperty;


@ApplicationScoped
public class ProducerService {
    
    private static Logger log = Logger.getLogger(ProducerService.class.getName());

    @ConfigProperty(name = "quarkus.pulsar.url")
    private String pulsarUrl;

    private PulsarClient pulsarClient;

    private Producer<String> stringProducer;

    @PostConstruct
    public void init() {
        try {
            log.info("init producer.");
            pulsarClient = PulsarClient.builder()
            .serviceUrl(pulsarUrl)
            .build();

            stringProducer = pulsarClient.newProducer(Schema.STRING)
            .topic("my-topic")
            .create();

        } catch (PulsarClientException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void sendMessage(String message) throws PulsarClientException {
        log.info("sending message to consumers ...");
        stringProducer.send(message);
    }

}
