package com.oelprince.pulsar;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@Path("/hello-resteasy")
public class ConsumerResource {

    Logger log = Logger.getLogger(getClass().getName());

    @ConfigProperty(name = "quarkus.message")
    private String message;


    @ConfigProperty(name = "quarkus.pulsar.url")
    private String pulsarUrl;


    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String hello() {

        log.log(Level.INFO, "Environment " + message); 

        try {

            PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarUrl)
                .build();

            Consumer consumer = client.newConsumer()
            .topic("my-topic")
            .subscriptionName("my-subscription")
            .subscribe();

            while (true) {
                // Wait for a message
                Message msg = consumer.receive();
                // Do something with the message
                System.out.printf("Message received: %s", new String(msg.getData()));
                // Acknowledge the message so that it can be deleted by the message broker
                consumer.acknowledge(msg);
            }

        } catch (PulsarClientException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return "Hello RESTEasy";
    }
}