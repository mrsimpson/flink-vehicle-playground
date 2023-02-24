package io.github.mrsimpson.vehicleStreaming.app;

import io.github.mrsimpson.vehicleStreaming.util.JsonSerialization;
import io.github.mrsimpson.vehicleStreaming.util.VehicleEvent;
import io.github.mrsimpson.vehicleStreaming.util.VehicleEventsGenerator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Date;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * A simple generator-only job.
 * It can be used to separate events generation from the actual pipeline processing
 * with communication via a kafka topic.
 * In order to achieve this, a kafka events topic needs to be configured at the processing pipeline job.
 */
public class VehicleEventsGenerationJob {

    static int fleetSize = 1;
    static int frequency = 1000;

    static int numberOfProviders = 1;

    static String topic = "events";

    static String kafkaUrl;

    public static void main(String[] args) throws Exception {
        // configure Job based on arguments
        Options options = new Options();
        options.addOption("fs", "fleetsize", true, "Size of each provider's fleet");
        options.addOption("fr", "frequency", true, "How fast shall 1min event time pass");
        options.addOption("p", "providers", true, "Number of provider");
        options.addOption("k", "kafka", true, "Kafka URL");
        options.addOption("t", "topic", true, "Kafka topic to send events to");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("kafka")) {
            kafkaUrl = cmd.getOptionValue("kafka");
        } else {
            throw new Exception("Kafka-URL required. This Job is made for generating events only and send the to Kafka.");
        }

        if (cmd.hasOption("fleetsize")) {
            int fleetSizeArg = Integer.parseInt(cmd.getOptionValue("fleetsize"));
            if (fleetSizeArg > 0) {
                fleetSize = fleetSizeArg;
            }
        }
        if (cmd.hasOption("frequency")) {
            int frequencyArg = Integer.parseInt(cmd.getOptionValue("frequency"));
            if (frequencyArg > 0) {
                frequency = frequencyArg;
            }
        }

        if (cmd.hasOption("providers")) {
            int providersArg = Integer.parseInt(cmd.getOptionValue("providers"));
            if (providersArg > 0) {
                numberOfProviders = providersArg;
            }
        }

        if (cmd.hasOption("topic")) {
            topic = cmd.getOptionValue("topic");
        }


        Logger.getLogger("stdout").log(new LogRecord(Level.WARNING, "Generator running " + new Date()));

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // propagate the params, so they can be accessed later using
        // ParameterTool parameters = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));


        // Set up the application based on the context (sources and sinks)
        SingleOutputStreamOperator<VehicleEvent> stream = env
                .addSource(new VehicleEventsGenerator(fleetSize, frequency))
                .setParallelism(numberOfProviders)
                .assignTimestampsAndWatermarks(VehicleEventsWatermarkStrategy.withOutOfOrderSeconds(30));
//                .assignTimestampsAndWatermarks(new VehicleEventsTimerAssigner()); // deprecated
        stream
                .name("raw-vehicle-events")
                .sinkTo(KafkaSink.<VehicleEvent>builder()
                        .setBootstrapServers(kafkaUrl)
                        .setRecordSerializer(
                                new KafkaRecordSerializationSchemaBuilder<VehicleEvent>()
                                        .setValueSerializationSchema(JsonSerialization.getJsonSerializationSchema())
                                        .setTopic(topic)
                                        .build()
                        )
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build());

        env.execute("Vehicle Generation");
    }
}
