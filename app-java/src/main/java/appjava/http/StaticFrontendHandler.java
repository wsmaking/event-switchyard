package appjava.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

public final class StaticFrontendHandler implements HttpHandler {
    private final Path distDir;

    public StaticFrontendHandler(Path distDir) {
        this.distDir = distDir;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        JsonHttpHandler.applyCors(exchange);
        if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(204, -1);
            exchange.close();
            return;
        }
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod()) && !"HEAD".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            exchange.close();
            return;
        }
        try {
            if (!Files.isDirectory(distDir)) {
                writePlain(exchange, 503, "frontend_dist_missing");
                return;
            }

            String requestPath = exchange.getRequestURI().getPath();
            Path resolved = resolveRequestedFile(distDir, requestPath);
            if (resolved == null || !Files.exists(resolved) || !Files.isRegularFile(resolved)) {
                writePlain(exchange, 404, "not_found");
                return;
            }

            byte[] body = Files.readAllBytes(resolved);
            exchange.getResponseHeaders().set("Content-Type", contentType(resolved));
            if (requestPath.startsWith("/assets/") || requestPath.endsWith(".js") || requestPath.endsWith(".css")) {
                exchange.getResponseHeaders().set("Cache-Control", "public, max-age=31536000, immutable");
            } else {
                exchange.getResponseHeaders().set("Cache-Control", "no-cache");
            }
            exchange.sendResponseHeaders(200, "HEAD".equalsIgnoreCase(exchange.getRequestMethod()) ? -1 : body.length);
            if (!"HEAD".equalsIgnoreCase(exchange.getRequestMethod())) {
                try (OutputStream outputStream = exchange.getResponseBody()) {
                    outputStream.write(body);
                }
            }
        } finally {
            exchange.close();
        }
    }

    static Path resolveRequestedFile(Path distDir, String requestPath) {
        String normalized = requestPath == null || requestPath.isBlank() ? "/" : requestPath;
        if ("/".equals(normalized) || isSpaRoute(normalized)) {
            return distDir.resolve("index.html").normalize();
        }
        String relative = normalized.startsWith("/") ? normalized.substring(1) : normalized;
        if (relative.contains("..")) {
            return null;
        }
        Path candidate = distDir.resolve(relative).normalize();
        if (!candidate.startsWith(distDir.normalize())) {
            return null;
        }
        if (Files.isDirectory(candidate)) {
            return candidate.resolve("index.html");
        }
        if (!hasExtension(relative) && !Files.exists(candidate)) {
            return distDir.resolve("index.html").normalize();
        }
        return candidate;
    }

    private static boolean isSpaRoute(String path) {
        return path.startsWith("/mobile") || !hasExtension(path.substring(1));
    }

    private static boolean hasExtension(String value) {
        int slashIndex = value.lastIndexOf('/');
        int dotIndex = value.lastIndexOf('.');
        return dotIndex > slashIndex;
    }

    private static String contentType(Path file) {
        String name = file.getFileName().toString().toLowerCase(Locale.ROOT);
        return switch (name.substring(Math.max(0, name.lastIndexOf('.')))) {
            case ".html" -> "text/html; charset=utf-8";
            case ".js" -> "text/javascript; charset=utf-8";
            case ".css" -> "text/css; charset=utf-8";
            case ".json", ".webmanifest" -> "application/json; charset=utf-8";
            case ".svg" -> "image/svg+xml";
            case ".png" -> "image/png";
            case ".ico" -> "image/x-icon";
            default -> "application/octet-stream";
        };
    }

    private static void writePlain(HttpExchange exchange, int statusCode, String body) throws IOException {
        byte[] bytes = body.getBytes();
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(bytes);
        }
    }
}
