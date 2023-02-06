package io.github.mrsimpson.vehicleStreaming.app;

import io.github.mrsimpson.vehicleStreaming.util.StdOutSink;
import io.github.mrsimpson.vehicleStreaming.util.VehicleEvent;
import io.github.mrsimpson.vehicleStreaming.util.VehicleEventsGenerator;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.commons.cli.*;

import java.util.Date;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class VehicleProcessingJob {

    static int fleetSize = 1;
    static int frequency = 1000;

    static int numberOfProviders = 1;

    static String kafkaUrl;

    private static <IN> Sink<IN> createSink(String identifier, String kafkaUrl) {
        if(kafkaUrl != null && !kafkaUrl.equals("")) {
            return KafkaSink.<IN>builder()
                    .setBootstrapServers(kafkaUrl)
                    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                            .setTopic(identifier.toLowerCase())
                            .setValueSerializationSchema(new JsonSerializationSchema<IN>())
                            .build()
                    )
                    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                    .build();
        }

        // default: StdOut
        return new StdOutSink<>(identifier);
    }

    public static void main(String[] args) throws Exception {
        // configure Job based on arguments
        Options options = new Options();
        options.addOption("fs", "fleetsize", true, "Size of each provider's fleet");
        options.addOption("fr", "frequency", true, "How fast shall 1min event time pass");
        options.addOption("p", "providers", true, "Number of provider");
        options.addOption("k", "kafka", true, "Kafka URL");

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

        Logger.getLogger("stdout").log(new LogRecord(Level.WARNING, "Läuft " + new Date()));

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // propagate the params, so they can be accessed later using
        // ParameterTool parameters = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));

        RichParallelSourceFunction<VehicleEvent> events = new VehicleEventsGenerator(fleetSize, frequency);

        // Set up the application based on the context (sources and sinks)
        VehicleStreamingPipeline app = new VehicleStreamingPipelineBuilder()
                .setEnv(env)
                .setRawVehicleEventsSink(createSink("Event", kafkaUrl))
                .setVehicleEvents(events)
                .setRentalsCountSink(createSink("Rentals", kafkaUrl))
                .setReturnsCountSink(createSink("Returns", kafkaUrl))
                .setTripSink(createSink("Trips", kafkaUrl))
                .createVehicleStreamingPipeline();

        app.run(numberOfProviders);
    }
}
