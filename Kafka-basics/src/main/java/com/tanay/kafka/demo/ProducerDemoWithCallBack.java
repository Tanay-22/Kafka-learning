package com.tanay.kafka.demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack
{
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallBack.class.getSimpleName());

    public static void main(String[] args)
    {
        log.info("I am a Kafka Producer");

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create Producer record

        for (int i = 0; i < 10; i++)
        {
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>("demo_java", "Hello Kafka ! " + i);

            // send the data asynchronous
            producer.send(producerRecord, new Callback()
            {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e)
                {
                    // executes everytime a record is successfully sent or an exception is thrown
                    if(e == null)
                    {
                        log.info("Received new metadata \n" +
                                "Topic : " + recordMetadata.topic() + "\n" +
                                "Partition : " + recordMetadata.partition() + "\n" +
                                "Offset : " + recordMetadata.offset() + "\n" +
                                "Timestamp : " + recordMetadata.timestamp()
                        );
                    }
                    else
                    {
                        log.error("Error while producing ", e);
                    }
                }
            });

            try
            {
                Thread.sleep(1000);
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
        // flush and close the producer
        producer.flush();
        producer.close();
    }
}
