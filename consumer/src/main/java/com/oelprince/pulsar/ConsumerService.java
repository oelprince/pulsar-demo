package com.oelprince.pulsar;

import java.util.logging.Logger;

import javax.annotation.PostConstruct;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.eclipse.microprofile.config.inject.ConfigProperty;

public class ConsumerService {

    private static Logger log = Logger.getLogger(ConsumerService.class.getName());
    
    @ConfigProperty(name = "quarkus.pulsar.url")
    private String pulsarUrl;
    private PulsarClient pulsarClient;
    private Consumer consumer;

    @PostConstruct
    public void init() {
        try {

            pulsarClient = PulsarClient.builder()
                .serviceUrl(pulsarUrl)
                .build();

            consumer = pulsarClient.newConsumer()
            .topic("my-topic")
            .subscriptionName("my-subscription")
            .subscribe();

           // PulsarListener thread = new PulsarListener(consumer);

           // thread.start();

            Runnable runnable =
                () -> { 
                    Message msg = null;
                    try {
                        while (true) {
                            // Wait for a message
                            msg = consumer.receive();
                            // Do something with the message
                            log.info(String.format("Message received: %s", new String(msg.getData())));
                            // Acknowledge the message so that it can be deleted by the message broker
                            consumer.acknowledge(msg);
                        }
                    } catch (PulsarClientException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                        if(msg != null)
                        consumer.negativeAcknowledge(msg);
                    }

                };

                Thread thread = new Thread(runnable);
                thread.start();
                

        } catch (PulsarClientException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
