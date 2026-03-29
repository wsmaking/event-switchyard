package appjava.order;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public final class ExecutionBenchmarkStore {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final Path stateFile;
    private final Map<String, ExecutionBenchmark> benchmarks;

    public ExecutionBenchmarkStore() {
        String stateRoot = System.getProperty(
            "app.state.dir",
            System.getenv().getOrDefault("APP_JAVA_STATE_DIR", "var/app-java")
        );
        this.stateFile = Path.of(stateRoot).resolve("execution-benchmarks.json");
        this.benchmarks = load();
    }

    public synchronized void put(ExecutionBenchmark benchmark) {
        benchmarks.put(benchmark.orderId(), benchmark);
        persist();
    }

    public synchronized Optional<ExecutionBenchmark> get(String orderId) {
        return Optional.ofNullable(benchmarks.get(orderId));
    }

    public synchronized void reset() {
        benchmarks.clear();
        persist();
    }

    private Map<String, ExecutionBenchmark> load() {
        try {
            Files.createDirectories(stateFile.getParent());
            if (!Files.exists(stateFile)) {
                return new LinkedHashMap<>();
            }
            Map<String, ExecutionBenchmark> loaded = OBJECT_MAPPER.readValue(
                stateFile.toFile(),
                new TypeReference<LinkedHashMap<String, ExecutionBenchmark>>() {
                }
            );
            return loaded == null ? new LinkedHashMap<>() : loaded;
        } catch (IOException ignored) {
            return new LinkedHashMap<>();
        }
    }

    private void persist() {
        try {
            Files.createDirectories(stateFile.getParent());
            Path tempFile = stateFile.resolveSibling(stateFile.getFileName() + ".tmp");
            OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValue(tempFile.toFile(), benchmarks);
            Files.move(tempFile, stateFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException ignored) {
        }
    }
}
