package com.oelprince.pulsar;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




public class PulsarThread extends Thread {
    private Logger log = LoggerFactory.getLogger(PulsarThread.class);
    
    private Consumer consumer;

    public PulsarThread(Consumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public void run() {
        Message msg = null;
        try {
            while (true) {
                // Wait for a message
                msg = consumer.receive();
                // Do something with the message
                consumer.acknowledge(msg);
            }
        } catch (PulsarClientException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            consumer.negativeAcknowledge(msg);
        }
    }
    
}
