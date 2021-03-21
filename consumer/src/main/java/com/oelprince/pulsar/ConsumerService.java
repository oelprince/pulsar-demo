package com.oelprince.pulsar;



import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.quarkus.runtime.Startup;

@Startup
public class ConsumerService {

    private static Logger log = LoggerFactory.getLogger(ConsumerService.class);
    
    @ConfigProperty(name = "quarkus.pulsar.url")
    private String pulsarUrl;
    @ConfigProperty(name = "quarkus.pulsar.pool-size")
    private int poolSize;
    private PulsarClient pulsarClient;
    private Consumer consumer;

    private ExecutorService executor;

    @PostConstruct
    public void init() {
        try {
            log.info("starting consumer service");
            pulsarClient = PulsarClient.builder()
                .serviceUrl(pulsarUrl)
                .build();

            consumer = pulsarClient.newConsumer()
            .topic("my-topic")
            .subscriptionName("my-subscription")
            .subscribe();

            executor = Executors.newFixedThreadPool(poolSize);

            for(int i=0; i < poolSize; i++) {
                Runnable consumerThread =
                    () -> { 
                        Message msg = null;
                        try {
                            while (true) {
                                // Wait for a message
                                msg = consumer.receive();
                                // Do something with the message
                                log.info("Message received: {}", new String(msg.getData()));
                                // Acknowledge the message so that it can be deleted by the message broker
                                consumer.acknowledge(msg);
                            }
                        } catch (PulsarClientException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                            if(msg != null) {
                                consumer.negativeAcknowledge(msg);
                            }
                        }

                    };
                
                    executor.execute(consumerThread);
                }
        } catch (PulsarClientException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
