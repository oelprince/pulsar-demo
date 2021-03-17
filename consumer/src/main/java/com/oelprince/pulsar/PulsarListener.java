package com.oelprince.pulsar;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;

public class PulsarListener extends Thread {

    
    private Consumer consumer;

    public PulsarListener(Consumer consumer) {
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
