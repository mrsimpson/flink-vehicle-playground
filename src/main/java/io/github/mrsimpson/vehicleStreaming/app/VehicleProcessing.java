package io.github.mrsimpson.vehicleStreaming.app;

import io.github.mrsimpson.vehicleStreaming.util.VehicleEventsGenerator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

import java.util.Date;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
public class VehicleProcessing {

	public static void main(String[] args) throws Exception {
		Logger.getLogger("stdout").log(new LogRecord(Level.WARNING, "Läuft " + new Date()));

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Set up the application based on the context (sources and sinks)
		VehicleStreamingApp app = new VehicleStreamingApp(env, new VehicleEventsGenerator(1, 1000), new PrintSinkFunction<>());
		app.run();
	}
}
