package com.oelprince.pulsar;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.eclipse.microprofile.config.inject.ConfigProperty;


@Path("/producer")
public class ProducerResource {
    Logger log = Logger.getLogger(getClass().getName());

    @Inject
    private ProducerService producerService;

    @GET
    @Path("{message}")
    @Produces(MediaType.TEXT_PLAIN)
    public String sendMsg(@PathParam("message") String message) {
        
        try {
            producerService.sendMessage(message);
        } catch (PulsarClientException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return "Message sent: " + message;
    }
}