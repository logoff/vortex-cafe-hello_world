# vortex-cafe-hello_world
This is a PrismTech Vortex Cafe Hello World with an arbitrary number of publishers and subscribers.

## Requisites

* Maven 3.x
* Oracle Java 7
* Prismtech Vortex Cafe 2.1.1 (with a valid configured license)

## Building
Execute:

```
mvn clean install
```

## Running it
Execute:

```
java -jar target/vortex.cafe.hello_world-0.0.1-SNAPSHOT-jar-with-dependencies.jar s1 p1
```

With 1 publisher and 1 subscriber threads.