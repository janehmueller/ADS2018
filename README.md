# Actor Database Systems 2018

## Run tests
```
# start sbt console
sbt
```
### Local tests
```
# run all tests
test

# run specific test
testOnly de.hpi.ads.remote.actors.PerformanceTest
```

### Multi-JVM tests
```
# run all tests
multi-jvm:test

# run specific test
multi-jvm:testOnly de.hpi.ads.MultiNodePerformanceTest
```

### Multi-Node tests
```
# run all tests
multiNodeTest

# run specific test
multiNodeTestOnly de.hpi.ads.MultiNodePerformanceTest
```
