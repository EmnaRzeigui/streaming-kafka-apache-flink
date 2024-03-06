package org.davidcampos.kafka.consumer;


import com.opencsv.CSVReader;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.davidcampos.kafka.commons.Commons;

import java.io.StringReader;
import java.util.Properties;

public class KafkaFlinkConsumerExample {
    private static final Logger logger = LogManager.getLogger(KafkaFlinkConsumerExample.class);

    public static void main(final String... args) {
        // Create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Properties
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Commons.EXAMPLE_KAFKA_SERVER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "FlinkConsumerGroup");

        DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer010<>(Commons.EXAMPLE_KAFKA_TOPIC, new SimpleStringSchema(), props));


        // Split up the lines in pairs (2-tuples) containing: (word,1)
        messageStream.flatMap(new Tokenizer())
                // group by the tuple field "0" and sum up tuple field "1"
                .keyBy(0)
                .sum(1)
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            logger.error("An error occurred.", e);
        }
    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            try (CSVReader csvReader = new CSVReader(new StringReader(value))) {
                // Read the line as an array of strings
                String[] tokens = csvReader.readNext();

                // Emit the pair (word, 1) for the first element of the line
                if (tokens != null && tokens.length > 0) {

                    int intValue = Integer.parseInt(tokens[10]);
                      // display person with stroke where age >40
                    if  (intValue == 1){
                        String word = tokens[1].trim(); // Assuming the first element is the "word"

                        out.collect(new Tuple2<>(word, 1));
                    }

                }
            } catch (Exception e) {
                // Handle the exception if CSV parsing fails
                e.printStackTrace();
            }
            }
        }
    }
