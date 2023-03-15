package io.github.mrsimpson.vehicleStreaming.app;

import io.github.mrsimpson.vehicleStreaming.util.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Date;
import java.util.Objects;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

class TimeEnabledJsonSerializationSchema<T> extends JsonSerializationSchema<T> {
    @Override
    public void open(InitializationContext context) {
        super.open(context);
        this.mapper.registerModule(new JavaTimeModule());
    }
}

public class VehicleProcessingJob {

    static int fleetSize = 1;
    static int frequency = 1000;

    static int numberOfProviders = 1;

    static String kafkaUrl;
    static String sourceTopic;

    private static <IN> Sink<IN> createSink(String identifier, String kafkaUrl) {
        JsonSerializationSchema<IN> jsonFormat = new TimeEnabledJsonSerializationSchema<>();
        if (kafkaUrl != null && !kafkaUrl.equals("")) {
            return KafkaSink.<IN>builder()
                    .setBootstrapServers(kafkaUrl)
                    .setRecordSerializer(
                            new KafkaRecordSerializationSchemaBuilder<>()
                                    .setValueSerializationSchema(jsonFormat)
                                    .setTopic(identifier.toLowerCase())
                                    .build()
                    )
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        }

        // default: StdOut
        return new StdOutSink<>(identifier);
    }

    private static FlinkKafkaConsumer<VehicleEvent> getVehicleEventsKafkaSource() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaUrl);
        properties.setProperty("group.id", "vehicle");


        return new FlinkKafkaConsumer<>(sourceTopic, new JsonDeserializationSchema<>(VehicleEvent.class), properties);
    }

    public static void main(String[] args) throws Exception {
        // configure Job based on arguments
        Options options = new Options();
        options.addOption("fs", "fleetsize", true, "Size of each provider's fleet");
        options.addOption("fr", "frequency", true, "How fast shall 1min event time pass");
        options.addOption("p", "providers", true, "Number of provider");
        options.addOption("k", "kafka", true, "Kafka URL");
        options.addOption("st", "sourcetopic", true, "Kafka source topic");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

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

        if (cmd.hasOption("kafka")) {
            kafkaUrl = cmd.getOptionValue("kafka");
        }

        Sink<VehicleEvent> rawEventsSink;
        if (cmd.hasOption("sourcetopic")) {
            sourceTopic = cmd.getOptionValue("sourcetopic");
            rawEventsSink = new NullSink<>();
        } else {
            createSink("Events", kafkaUrl);
            rawEventsSink = createSink("Events", kafkaUrl);
        }

        Logger.getLogger("stdout").log(new LogRecord(Level.WARNING, "Läuft " + new Date()));

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // propagate the params, so they can be accessed later using
        // ParameterTool parameters = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));

        RichParallelSourceFunction<VehicleEvent> events;
        if(!Objects.equals(sourceTopic, null)) {
            events = getVehicleEventsKafkaSource();
        } else {
            events = new VehicleEventsGenerator(fleetSize, frequency);
        }

        // Set up the application based on the context (sources and sinks)
        VehicleStreamingPipeline app = new VehicleStreamingPipelineBuilder()
                .setEnv(env)
                .setRawVehicleEventsSink(rawEventsSink)
                .setVehicleEvents(events)
                .setRentalsCountSink(createSink("Rentals", kafkaUrl))
                .setReturnsCountSink(createSink("Returns", kafkaUrl))
                .setTripSink(createSink("Trips", kafkaUrl))
                .setParkingSink(createSink("Parkings", kafkaUrl))
                .setAvailabilitySink(createSink("Availability", kafkaUrl))
                .createVehicleStreamingPipeline();

        app.run(numberOfProviders);
    }
}
