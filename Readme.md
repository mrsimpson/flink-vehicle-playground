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

### Using the [Flink Kubernetes Operator](https://github.com/apache/flink-kubernetes-operator)

Your application will be defined using a CRD and deployed to your (local) K8S-cluster.
In order to do this, we will package the flink application along with its job manager ("the flink framework"") into a docker image.
Since the artifacts in this repo have been made configurable, this allows for packaging your flink app with multiple different Flink versions.

1. Install the Flink K8S-operator as [described in the quickstart](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/try-flink-kubernetes-operator/quick-start/)
   `tl;dr:`
   ```bash
   kubectl create -f https://github.com/jetstack/cert-manager/releases/download/v1.8.2/cert-manager.yaml
   ```
   Wait for the cert manager pod to be ready!
   ```bash
   helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.3.1/
   helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator --namespace flink --create-namespace
   ```
2. Build and packages your app with maven as a shaded jar, optionally passing a Flink version:
    ```bash
    mvn clean install package
   ``` 

    or for a specific version

   ```bash
    mvn -Dflink.version=1.15.3 clean install package
    ```
3. Build a docker image referencing the `jar` just created:
    ```bash
   docker build -t flink-vehicle-example:latest .
    ```
    
    or for a specific version
    
   ```bash
   docker build -t flink-vehicle-example:latest --build-arg FLINK_VERSION=1.15.3 .
    ```
   
4. Send it to the cluster (you need to specify a version explicitly)

    ```bash
    FLINK_VERSION=v1_16 envsubst < k8s/vehicle-processing.yaml | kubectl apply --namespace=flink -f - 
    ```
   
_CAUTION: When deleting the Flink deployment, don't delete the pod, but delete the CRD `flinkdeployment`. 
Else you might end up with a non-deletable CRD._

## Generation only

In order to simulate a real world data ingestion via Kafka, you can deploy the generation as a separate application
Since there are multiple main classes within the same file, the local build works as above, just supply a different 
main class when deploying it:

```bash
FLINK_VERSION=v1_16 envsubst < k8s/vehicle-generation.yaml | kubectl apply --namespace=flink -f - 
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