package nextflow.file.http

import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

import groovy.json.JsonBuilder
import groovy.json.JsonSlurper

class GQLClient {
    private String endpoint
    private HttpClient client
    private HttpRequest.Builder requestBuilder

    class GQLQueryException extends Exception {
        GQLQueryException(String msg) {
            super(msg)
        }
    }

    GQLClient() {
        endpoint = "https://vacuole.latch.bio/graphql"

        String domain = System.getenv("LATCH_SDK_DOMAIN")
        if (domain != null) {
            endpoint = "https://vacuole.$domain/graphql"
        }

        this.endpoint = endpoint
        this.client = HttpClient.newHttpClient()
        this.requestBuilder =  HttpRequest.newBuilder()
            .uri(URI.create(this.endpoint))
            .header("Content-Type", "application/json")
            .header("Authorization", LatchPathUtils.getAuthHeader())
    }

    Map execute(String query) {
        return execute(query, [:])
    }

    // todo(ayush): add validation
    Map execute(String query, Map<String, String> variables) {
        JsonBuilder builder = new JsonBuilder()
        builder(["query": query, "variables": variables])

        HttpRequest req = this.requestBuilder.POST(HttpRequest.BodyPublishers.ofString(builder.toString())).build()
        HttpResponse<String> response = this.client.send(req, HttpResponse.BodyHandlers.ofString())

        JsonSlurper slurper = new JsonSlurper()
        Map<String, Map> responseObj = slurper.parseText(response.body()) as Map<String, Map>

        if (responseObj.containsKey("error")) {
            throw new GQLQueryException(responseObj["error"].toString())
        }

        return responseObj["data"]
    }
}
