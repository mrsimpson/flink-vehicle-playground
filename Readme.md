# What's in this repo

This repo is a sample of stream processing of geographical events (e. g. emitted by vehicles).
It serves as playground for getting to learn using apache flink to perform streaming aggregations, 
sink results to configurable targets and observe how flink parallelizes execution and distributes state.

# Development

## Just run it

### Within your  IDE

- Use an [IntelliJ IDEA](https://www.jetbrains.com/de-de/idea/download/) (frankly, I ❤️ VSCode, but the dev ex with IDEA for Java is so much better. It just works.)
- Check out this repo, open the `VehicleProcessingJob.java` file and run / debug it.

### On you local cluster

```bash
mvn clean install package

$FLINK_PATH/bin/flink run \
  -c io.github.mrsimpson.vehicleStreaming.app.VehicleProcessingJob \
  target/flink-vehicle-example-0.1.jar \
  --fleetsize=1000  \
  --frequency=500
```

## Develop it

- Buy the fantastic book by @fhueske [Stream Processing with Apache Flink](https://www.oreilly.com/library/view/stream-processing-with/9781491974285/)

This project has been scaffolded using a generator
```
mvn archetype:generate                            \
   -DarchetypeGroupId=org.apache.flink            \
   -DarchetypeArtifactId=flink-quickstart-java   \
   -DarchetypeVersion=1.15.                       \
   -DinteractiveMode=true
```