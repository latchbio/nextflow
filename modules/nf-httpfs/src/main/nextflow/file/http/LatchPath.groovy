package nextflow.file.http

import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.ProviderMismatchException

import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import org.apache.commons.lang.NotImplementedException

class LatchPath extends XPath {
    LatchFileSystem fs
    Path path

    LatchPath(LatchFileSystem fs, String path) {
        super(fs, path)

        this.fs = fs
        this.path = Paths.get(path)
    }

    LatchPath(LatchFileSystem fs, String path, String[] more) {
        super(fs, path, more)

        this.fs = fs
        this.path = Paths.get(path, more)
    }

    @Override
    URI toUri() {
        return new URI("latch", fs.domain, path.toString(), null, null)
    }

    @Override
    Path getName(int index) {
        return new LatchPath(null, path.getName(index).toString())
    }

    @Override
    Path subpath(int beginIndex, int endIndex) {
        return new LatchPath(null, path.subpath(beginIndex, endIndex).toString())
    }

    @Override
    Path normalize() {
        return new LatchPath(fs, path.normalize().toString())
    }

    @Override
    Path resolve(Path other) {
        if (this.class != other.class)
            throw new ProviderMismatchException()

        def that = (LatchPath) other

        if (that.fs != null && this.fs != that.fs ) {
            return other
        } else if (that.path != null) {
            def newPath = this.path.resolve(that.path)
            return new LatchPath(fs, newPath.toString())
        } else {
            return this
        }
    }

    @Override
    Path resolveSibling(Path other) {
        if (this.class != other.class)
            throw new ProviderMismatchException()

        def that = (LatchPath) other

        if (that.fs != null && this.fs != that.fs ) {
            return other
        } else if (that.path != null) {
            final Path newPath = this.path.resolveSibling(that.path)
            return newPath.isAbsolute() ? new LatchPath(fs, newPath.toString()) : new LatchPath(null, newPath.toString())
        } else {
            return this
        }
    }

    Object startUpload(Path local) {
        String cluster = System.getenv("LATCH_SDK_DOMAIN") ?: "latch.bio"
        String endpoint = "https://nucleus.$cluster/get-signed-url"



    }

    URL getSignedURL() {
        String cluster = System.getenv("LATCH_SDK_DOMAIN") ?: "latch.bio"
        String endpoint = "https://nucleus.$cluster/get-signed-url"

        JsonBuilder builder = new JsonBuilder()
        builder(["path": this.toUri().toString()])

        def client = HttpClient.newHttpClient()
        def request =  HttpRequest.newBuilder()
            .uri(URI.create(endpoint))
            .header("Content-Type", "application/json")
            .header("Authorization", LatchPathUtils.getAuthHeader())
            .POST(HttpRequest.BodyPublishers.ofString(builder.toString()))
            .build()

        def response = client.send(request, HttpResponse.BodyHandlers.ofString())

        if (response.statusCode() != 200) {
            throw new FileNotFoundException()
        }

        def slurper = new JsonSlurper()
        def json = slurper.parseText(response.body())

        return new URL(json["data"]["url"] as String)
    }
}
