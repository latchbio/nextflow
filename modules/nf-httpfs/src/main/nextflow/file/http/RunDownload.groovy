package nextflow.file.http

import groovy.json.JsonBuilder
import groovy.json.JsonSlurper

import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.ThreadPoolExecutor

class RunDownload {
    private final long downloadPartSize = 100 * 1024 * 1024
    private final long downloadChunkSize = 1 * 1024 * 1024

    URI toUri(Path path) {
        return new URI("latch", "24030.account", path.toString(), null, null)
    }

    URL getSignedURL(Path path) {
        JsonBuilder builder = new JsonBuilder()
        builder(["path": toUri(path).toString()])

        HttpRetryClient client = new HttpRetryClient()
        def request =  HttpRequest.newBuilder()
            .uri(URI.create("https://nucleus.latch.bio/ldata/get-signed-url"))
            .header("Content-Type", "application/json")
            .header("Authorization", LatchPathUtils.getAuthHeader())
            .POST(HttpRequest.BodyPublishers.ofString(builder.toString()))
            .build()

        def response = client.send(request)

        if (response.statusCode() != 200) {
            throw new FileNotFoundException(path.toString())
        }

        def slurper = new JsonSlurper()
        def json = slurper.parseText(response.body())

        return new URL(json["data"]["url"] as String)
    }

    private int downloadPart(FileChannel outputStream, HttpRetryClient client, URL url, long start, long end, long part) {
        def req =  HttpRequest.newBuilder()
            .uri(url.toURI())
            .header("Range", "bytes=${start}-${end}")
            .GET()
            .build()

        def response = client.stream(req)
        InputStream inputStream = response.body()

        try {
            long bytesWritten = 0
            byte[] buffer = new byte[downloadChunkSize]

            int bytesRead
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, 0, bytesRead)
                bytesWritten += outputStream.write(byteBuffer, start + bytesWritten)
            }
        } finally {
            inputStream.close()
        }

        return 0
    }

    void downloadParallel(Path path, Path local) {
        def url = getSignedURL(path)

        HttpRetryClient client = new HttpRetryClient()
        def request =  HttpRequest.newBuilder()
            .uri(url.toURI())
            .header("Range", "bytes=0-0")
            .GET()
            .build()

        def response = client.send(request)
        if (![200, 206].contains(response.statusCode()))
            throw new Exception("Failed to get file size: ${response.statusCode()}")

        def contentRange = response.headers().firstValue('Content-Range')

        def byteRangePrefix = "bytes 0-0/"
        if (contentRange.empty || !contentRange.get().startsWith(byteRangePrefix))
            throw new Exception("Failed to get file size: Content-Range invalid ${contentRange.get()}")

        long fileSize = Long.parseLong(contentRange.get().substring(byteRangePrefix.length()))
        long numParts = (long) Math.ceil(fileSize / (double) downloadPartSize)

        if (local.exists())
            Files.delete(local)

        FileChannel outputStream = FileChannel.open(local, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
        outputStream.truncate(fileSize)

        ThreadPoolExecutor executor = Executors.newFixedThreadPool(20)

        try {
            ArrayList<Future> futures = []

            for (long i = 0; i < numParts; i++) {
                long start = i * downloadPartSize
                long end = Math.min(start + downloadPartSize - 1, fileSize - 1)
                long part = i

                futures << executor.submit {
                    downloadPart(outputStream, client, url, start, end, part)
                }
            }

            futures.each { it.get() }
        } finally {
            outputStream.close()
        }
    }

    void download(Path path, Path local) {
        println "Sync download started for ${path.toUriString()}"
        def url = getSignedURL(path)

        HttpRetryClient client = new HttpRetryClient()
        def req =  HttpRequest.newBuilder()
            .uri(url.toURI())
            .GET()
            .build()

        def resp = client.stream(req)
        InputStream inputStream = resp.body()

        FileChannel outputStream = FileChannel.open(local, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

        try {
            long bytesWritten = 0
            byte[] buffer = new byte[downloadChunkSize]
            int bytesRead
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, 0, bytesRead)
                bytesWritten += outputStream.write(byteBuffer, bytesWritten)
            }
        } finally {
            inputStream.close()
        }
    }

    static void main(String[] args) {
        RunDownload d = new RunDownload()
        d.downloadParallel(Path.of("/broad-references/hg38/v0/CrossSpeciesContamination/CrossSpeciesContaminant/meats.fa"), Path.of("./file.txt"))
    }
}
