# Copilot Instructions for reactive-redis-coffe

## Repository Overview

**Project Type**: Spring Boot 2.1.6 reactive web application with Redis caching  
**Language**: Java 8 (JDK 1.8)  
**Build Tool**: Maven 3.x  
**Framework**: Spring Boot with Spring WebFlux and Spring Data Redis Reactive  
**Size**: Small (~6 Java files, 25MB with dependencies)  
**Main Class**: `com.github.pedrobacchini.reactiverediscoffe.ReactiveRedisCoffeApplication`

This is a demo project showcasing reactive programming with Redis using Spring Boot. The application provides REST endpoints for managing coffee data stored in Redis, with reactive streams for real-time updates via Server-Sent Events (SSE).

## Critical Build Requirements

### Java Version Requirement (CRITICAL)
**ALWAYS use Java 8 (JDK 1.8) - the build WILL FAIL with newer Java versions.**

The project uses Lombok with an older version that is incompatible with Java 11+. Using Java 17 (system default) will cause this error:
```
Fatal error compiling: java.lang.IllegalAccessError: class lombok.javac.apt.LombokProcessor
cannot access class com.sun.tools.javac.processing.JavacProcessingEnvironment
```

**Required commands before ANY Maven operation:**
```bash
export JAVA_HOME=/usr/lib/jvm/temurin-8-jdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

Verify Java version before building:
```bash
java -version  # Must show "1.8.0"
```

### Redis Requirement
- **Compilation**: Works without Redis (use `-DskipTests` to skip tests)
- **Testing**: Requires Redis running on localhost:6379
- **Running**: Requires Redis running on localhost:6379

To start Redis:
```bash
sudo service redis-server start
redis-cli ping  # Should return PONG
```

## Build Commands (Validated Sequence)

### 1. Clean the Project
```bash
export JAVA_HOME=/usr/lib/jvm/temurin-8-jdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
mvn clean
```
**Time**: ~4-5 seconds  
**No Redis required**

### 2. Compile Only (No Tests)
```bash
export JAVA_HOME=/usr/lib/jvm/temurin-8-jdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
mvn clean compile
```
**Time**: ~10-15 seconds (first run with dependency download)  
**No Redis required**  
**Success indicator**: `BUILD SUCCESS` with 5 source files compiled

### 3. Run Tests (Requires Redis)
```bash
# Ensure Redis is running first
sudo service redis-server start
redis-cli ping  # Verify connection

export JAVA_HOME=/usr/lib/jvm/temurin-8-jdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
mvn test
```
**Time**: ~5-10 seconds  
**Requires**: Redis running on localhost:6379  
**Test Count**: 1 test (context load test)  
**Expected Output**: Coffee data printed to console during test

### 4. Package Application
```bash
export JAVA_HOME=/usr/lib/jvm/temurin-8-jdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
mvn package
```
**Time**: ~11-15 seconds  
**Requires**: Redis running (for tests)  
**Output**: Creates `target/reactive-redis-coffe-0.0.1-SNAPSHOT.jar`

### 5. Run Application
```bash
export JAVA_HOME=/usr/lib/jvm/temurin-8-jdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
mvn spring-boot:run
```
**Port**: 7878 (configured in application.properties)  
**Requires**: Redis running on localhost:6379  
**Endpoints**:
- GET `/sse` - Server-Sent Events stream of coffee updates
- POST `/` - Add new coffee (accepts JSON: `{"name": "Coffee Name"}`)

## Project Structure

### Key Files
```
.
├── pom.xml                          # Maven build configuration
├── .gitignore                       # Git ignore rules
└── src/
    ├── main/
    │   ├── java/com/github/pedrobacchini/reactiverediscoffe/
    │   │   ├── ReactiveRedisCoffeApplication.java    # Main application entry point
    │   │   ├── domain/
    │   │   │   └── Coffee.java                        # Domain model (uses Lombok @Data)
    │   │   ├── config/
    │   │   │   ├── CoffeeConfiguration.java           # Redis connection & serialization config
    │   │   │   └── CoffeLoader.java                   # @PostConstruct data loader (seeds Redis)
    │   │   └── resource/
    │   │       └── CoffeController.java               # REST controller with SSE endpoint
    │   └── resources/
    │       └── application.properties                 # Server port: 7878
    └── test/
        └── java/com/github/pedrobacchini/reactiverediscoffe/
            └── ReactiveRedisCoffeeApplicationTests.java  # Basic context load test
```

### Architecture Components
1. **Domain**: `Coffee` entity with id and name (Lombok-annotated)
2. **Configuration**: Redis connection factory (localhost:6379) and reactive template setup
3. **Data Loader**: `CoffeLoader` populates Redis with 3 coffee entries on startup
4. **Controller**: REST endpoints with reactive publishers using `ReplayProcessor` for SSE
5. **Redis Integration**: Reactive Redis operations with JSON serialization via Jackson

### Dependencies (from pom.xml)
- spring-boot-starter-data-redis-reactive
- spring-boot-starter-webflux
- spring-boot-devtools (runtime, optional)
- lombok (optional, annotation processor)
- spring-boot-starter-test (test scope)
- reactor-test (test scope)

## Common Issues & Solutions

### Build fails with IllegalAccessError
**Cause**: Not using Java 8  
**Solution**: Set JAVA_HOME to Java 8 as shown above

### Tests fail with "Unable to connect to Redis"
**Cause**: Redis not running  
**Solution**: Start Redis with `sudo service redis-server start`

### Application fails to start
**Cause**: Redis not available or port 7878 in use  
**Solution**: Check Redis status and ensure port 7878 is free

## Validation Steps

After making code changes:
1. **Set Java 8 environment** (always first step)
2. **Ensure Redis is running** (for tests/runtime)
3. **Clean and compile**: `mvn clean compile`
4. **Run tests**: `mvn test` (if tests exist for your changes)
5. **Package**: `mvn package` to create executable JAR
6. **Manual verification**: Run with `mvn spring-boot:run` and test endpoints with curl

## Important Notes

- **No GitHub Actions/CI**: This repository has no automated CI/CD workflows
- **No linting configuration**: No checkstyle, PMD, or similar tools configured
- **Minimal test coverage**: Only one basic test exists
- **Development mode**: Uses spring-boot-devtools for hot reload
- **Redis configuration**: Hardcoded to localhost:6379 in `CoffeeConfiguration.java`
- **Server port**: Configured to 7878 in `application.properties` (not default 8080)
- **Reactive patterns**: Uses Project Reactor with `ReplayProcessor` for event streaming

## Trust These Instructions

These instructions have been validated by executing each command in the actual environment. Only search for additional information if these instructions are incomplete or you encounter errors not documented here.
