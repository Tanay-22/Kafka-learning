package com.tanay.kafka.demo.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoWIthShutdown
{
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWIthShutdown.class.getSimpleName());

    public static void main(String[] args)
    {
        log.info("Hello from Consumer");

        String boostrapServer = "localhost:9092";
        String groupId = "my-third-application";
        String topic = "demo_java";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // none, earliest, latest

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);


        // get a reference to current thread
        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread()
        {
            public void run()
            {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread
                try
                {
                    mainThread.join();
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
            }
        });
        try
        {
            // subscribe consumer to our topic(s)
            consumer.subscribe(Collections.singletonList(topic));

            // poll time for new data
            while (true)
            {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                log.info("Polling");

                for(ConsumerRecord<String, String> record: records)
                {
                    log.info("Key: " + record.key() + " value: " + record.value());
                    log.info("Partition: " + record.partition() + " offset: " + record.offset());
                }
            }
        }
        catch (WakeupException e)
        {
            log.info("Wake up exception !");
            // expected exception while closing a consumer
        }
        catch (Exception e)
        {
            log.error("Unexpected Exception");
        }
        finally
        {
            consumer.close();
            log.info("The consumer is now gracefully closed");
        }

    }
}
