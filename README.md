# spark-geo-privacy

Geospatial privacy functions for Apache Spark 1.6

## Algorithms

- Geo-indistinguishability (Andrés, Miguel E., et al. [Geo-indistinguishability: Differential privacy for location-based systems.](http://dl.acm.org/citation.cfm?id=2516735))
- Geo-indistinguishability for traces (K. Chatzikokolakis, et al. [A Predictive Differentially-Private Mechanism for Mobility Traces.](https://petsymposium.org/2014/papers/Chatzikokolakis.pdf))
- Stay point detection (Q. Li, et al. [Mining user similarity based on location history.](http://dl.acm.org/citation.cfm?id=1463477))
- Geo point recall (Primault, Vincent, et al. [Time Distortion Anonymization for the Publication of Mobility Data with High Utility.](https://arxiv.org/pdf/1507.00443))

## Usage

Build the package and include the output JAR when submitting your Spark application.

### Building

```bash
# build the package
./build/sbt assembly

# run the tests
./build/sbt test

# publish to the local repository
./build/sbt publish-local
```

## Notes

This spark package is build using [`sbt-spark-package`](https://github.com/databricks/sbt-spark-package).

## Contributors

- [@chrissng](https://github.com/chrissng)
- [@leesq](https://github.com/leesq)
- [@yxtay](https://github.com/yxtay)
