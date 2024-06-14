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

        String error = ""

        for (int i = 0; i < retries; i++) {
            HttpResponse<String> response
            try {
                response = client.send(request, HttpResponse.BodyHandlers.ofString())
            } catch (IOException e) {
                sleep(2 ** (i + 1) * 5000)
                error = e.message
                continue
            }


            def statusCode = response.statusCode()
            if (statusCode != 200) {
                if (statusCode == 429 || statusCode >= 500) {
                    sleep(2 ** (i + 1) * 5000)
                    error = response.body()
                    continue
                }

                break
            }

            return response
        }

        throw new Exception("Failed to upload file: ${error}")
    }
}
