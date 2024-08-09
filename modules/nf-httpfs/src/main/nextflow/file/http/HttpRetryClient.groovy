package nextflow.file.http

import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

class HttpRetryClient {

    private HttpClient client

    HttpRetryClient() {
        this.client = HttpClient.newHttpClient()
    }

    HttpResponse _send(HttpRequest request, boolean stream = false, int retries = 3) {
        if (retries <= 0) {
            throw new RuntimeException("failed to submit request, retries must be > 0")
        }

        Exception error
        HttpResponse response

        for (int i = 0; i < retries; i++) {
            if (i != 0) {
                sleep(2 ** i * 5000)
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
        return (HttpResponse<InputStream>) _send(request, true, retries)
    }

    HttpResponse<String> send(HttpRequest request, int retries = 3) {
        return (HttpResponse<String>) _send(request, false, retries)
    }
}
