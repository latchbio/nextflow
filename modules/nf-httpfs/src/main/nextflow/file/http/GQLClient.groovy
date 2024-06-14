package nextflow.file.http

import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

import groovy.json.JsonBuilder
import groovy.json.JsonSlurper

class GQLClient {
    private String endpoint
    private HttpRetryClient client

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
        this.client = new HttpRetryClient()
    }

    Map execute(String query) {
        return execute(query, [:])
    }

    // todo(ayush): add validation
    Map execute(String query, Map variables) {
        JsonBuilder builder = new JsonBuilder()
        builder(["query": query, "variables": variables])

        HttpRequest.Builder requestBuilder =  HttpRequest.newBuilder()
            .uri(URI.create(this.endpoint))
            .header("Content-Type", "application/json")
            .header("Authorization", LatchPathUtils.getAuthHeader())
        HttpRequest req = requestBuilder.POST(HttpRequest.BodyPublishers.ofString(builder.toString())).build()
        HttpResponse<String> response = this.client.send(req)

        JsonSlurper slurper = new JsonSlurper()
        Map<String, Map> responseObj = slurper.parseText(response.body()) as Map<String, Map>

        if (responseObj.containsKey("errors")) {
            throw new GQLQueryException(
                "query failed: variables=${variables.toString()} errors=${responseObj['errors'].toString()}"
            )
        }

        return responseObj["data"]
    }
}
