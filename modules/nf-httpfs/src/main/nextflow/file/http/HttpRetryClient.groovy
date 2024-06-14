package nextflow.file.http

import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

class HttpRetryClient {

    private HttpClient client

    HttpRetryClient() {
        this.client = HttpClient.newHttpClient()
    }

    HttpResponse<String> send(HttpRequest request, int retries = 3) {
        if (retries <= 0) {
            throw new RuntimeException("failed to submit request, retries must be > 0")
        }

        Exception error
        HttpResponse<String> response

        for (int i = 0; i < retries; i++) {
            if (i != 0) {
                sleep(2 ** i * 5000)
            }

            error = null
            try {
                response = client.send(request, HttpResponse.BodyHandlers.ofString())
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
}
