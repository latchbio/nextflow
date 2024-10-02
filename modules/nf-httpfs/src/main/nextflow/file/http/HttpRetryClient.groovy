package nextflow.file.http

import groovy.util.logging.Slf4j

import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.ConnectException
import java.time.Duration

@Slf4j
class HttpRetryClient {

    transient private HttpClient client = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build()

    HttpResponse sendHelper(HttpRequest request, boolean stream = false, int retries = 3) {
        if (retries <= 0) {
            throw new RuntimeException("failed to submit request, retries must be > 0")
        }

        Exception error
        HttpResponse response

        for (int i = 0; i < retries; i++) {
            if (i != 0) {
                log.debug "[${i}/${retries}] Request to ${request.uri()} failed. Retrying..."
                sleep(2 ** (i + 1) * 5000)
            }

            error = null
            try {
                response = client.send(request, stream ? HttpResponse.BodyHandlers.ofInputStream() : HttpResponse.BodyHandlers.ofString())
            } catch (IOException e) {
                error = e
                continue
            }

            def statusCode = response.statusCode()
            if (statusCode == 429 || statusCode >= 500)
                continue

            break
        }

        if (error != null) {
            throw error
        }

        return response
    }

    HttpResponse<InputStream> stream(HttpRequest request, int retries = 3) {
        return (HttpResponse<InputStream>) sendHelper(request, true, retries)
    }

    HttpResponse<String> send(HttpRequest request, int retries = 3) {
        return (HttpResponse<String>) sendHelper(request, false, retries)
    }
}
