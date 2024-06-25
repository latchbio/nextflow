package nextflow.file.http

import java.net.http.HttpRequest
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.ProviderMismatchException
import java.util.concurrent.CompletionService
import java.util.concurrent.ExecutorCompletionService

import groovy.json.JsonBuilder
import groovy.json.JsonGenerator
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j

@Slf4j
class LatchPath extends XPath {
    LatchFileSystem fs
    Path path

    private static String cluster = System.getenv("LATCH_SDK_DOMAIN") ?: "latch.bio"
    private static String host = "https://nucleus.${cluster}"

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
    Path getRoot() {
        return new LatchPath(fs, "/")
    }

    @Override
    Path getParent() {
        String result = path.parent ? path.parent.toString() : null
        if( result ) {
            if( result != '/' ) result += '/'
            return new LatchPath(fs, result)
        }
        return null
    }

    @Override
    Path getName(int index) {
        return new LatchPath(fs, path.getName(index).toString())
    }

    @Override
    Path subpath(int beginIndex, int endIndex) {
        return new LatchPath(fs, path.subpath(beginIndex, endIndex).toString())
    }

    @Override
    Path normalize() {
        return new LatchPath(fs, path.normalize().toString())
    }

    @Override
    Path relativize(Path other) {
        if (this.class != other.class) {
            def newPath = path.relativize(other)
            return new LatchPath(fs, newPath.toString())
        }

        def that = (LatchPath) other

        if (this.fs.domain != that.fs.domain) {
            throw new UnsupportedOperationException("Cannot relativize two files in different account roots: ${this.toUriString()}, ${that.toUriString()}")
        }

        if (this.path == that.path) {
            return new LatchPath(this.fs, "")
        }

        def thisParts = this.path.iterator().toList()
        def thatParts = that.path.iterator().toList()

        int start = 0
        for (Path part: thisParts) {
            if (part != thatParts[start]) {
                break
            }

            start++
        }

        if (start >= thatParts.size())
            throw new RuntimeException("failed to relativize path")

        Path res = Paths.get("")
        for (int i = start; i < thisParts.size(); i++)
            res = res.resolve("..")

        for (int i = start; i < thatParts.size(); i++)
            res = res.resolve(thatParts[i].name)

        return new LatchPath(this.fs, res.toString())
    }

    @Override
    Path resolve(Path other) {
        if (this.class != other.class) {
            def newPath = path.resolve(other.toString())
            return new LatchPath(fs, newPath.toString())
        }

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

    boolean exists() {
        try {
            this.fileSystem.provider().readAttributes(this, LatchFileAttributes)
        } catch (NoSuchFileException ignored) {
            return false
        }
        return true
    }

    private final long defaultChunkSize = 5 * 1024 * 1024
    private final long max_parts = 10000
    private final long max_upload_size = 5497558138880 // 5 * 1024 * 1024 * 1024 * 1024, cant put this though bc groovy is quirky and integer overflows

    class CompletedPart {
        long PartNumber
        String ETag

        CompletedPart(long PartNumber, String ETag) {
            this.PartNumber = PartNumber
            this.ETag = ETag
        }
    }

    void upload(Path local) {
        if (!local.exists()) {
            throw new Exception("Could not find path ${local} to upload")
        }

        if (!local.isFile()) {
            throw new UnsupportedOperationException("Uploading directories is not currently supported")
        }

        def size = local.size()
        if (size > max_upload_size) {
            double size_tib = (size as double) / (1024 * 1024 * 1024 * 1024)
            throw new Exception("File $local is too large to upload ($size_tib TiB, maximum upload size is 5 TiB)")
        }

        def mimeType = Files.probeContentType(local) ?: "application/octet-stream"

        long chunkSize = defaultChunkSize

        long numParts = size.intdiv(chunkSize)
        if (size % chunkSize != 0) {
            numParts = numParts + 1
        }

        if (numParts > max_parts) {
            numParts = max_parts
            chunkSize = size.intdiv(numParts)
            if (size % numParts != 0) {
                chunkSize = chunkSize + 1
            }
        }

        JsonBuilder builder = new JsonBuilder()
        builder(["path": this.toUri().toString(), "part_count": numParts, "content_type": mimeType])

        HttpRetryClient client = new HttpRetryClient()
        def request =  HttpRequest.newBuilder()
            .uri(URI.create("${host}/ldata/start-upload"))
            .header("Content-Type", "application/json")
            .header("Authorization", LatchPathUtils.getAuthHeader())
            .POST(HttpRequest.BodyPublishers.ofString(builder.toString()))
            .build()

        def response = client.send(request)

        if (response.statusCode() != 200) {
            throw new Exception("Failed to upload file: ${response.body()}")
        }

        def slurper = new JsonSlurper()
        Map json = slurper.parseText(response.body()) as Map
        Map data = json.get("data") as Map

        if (data.containsKey("version_id")) {
            return // file is empty, nothing to upload
        }

        List<String> urls = data.get("urls") as List<String>
        String uploadId = data.get("upload_id") as String

        def file = FileChannel.open(local)
        CompletionService<CompletedPart> cs = new ExecutorCompletionService<CompletedPart>(this.fs.provider.executor)

        long partIndex = 0
        List<CompletedPart> parts = new ArrayList<CompletedPart>(numParts as int)
        for (String url: urls) {
            partIndex++
            def idx = partIndex
            def chunkUrl = url

            cs.submit {
                // casting cur_chunk_size to int is fine here as cur_chunk_size will never
                // be > 2^31 - 1 (this would require a file larger than the max size of 5 TiB)
                def buf = ByteBuffer.allocate(chunkSize as int)

                def pos = chunkSize * (idx - 1)
                def bytes_read = file.read(buf, pos)

                byte[] arr = buf.array()
                if (bytes_read < chunkSize) {
                    arr = Arrays.copyOfRange(arr, 0, bytes_read)
                }

                HttpRequest req =  HttpRequest.newBuilder()
                    .uri(URI.create(chunkUrl))
                    .PUT(HttpRequest.BodyPublishers.ofByteArray(arr))
                    .build()

                def resp = client.send(req)

                String etag = resp.headers().firstValue("ETag").get().replace("\"", "") // ayush: no idea why but ETag has quotes sometimes
                def res = new CompletedPart(idx, etag)
                return res
            }

        }

        for (String url: urls) {
            CompletedPart res = cs.take().get()
            parts[res.PartNumber - 1] = res
        }

        file.close()

        // necessary bc otherwise "PartNumber" gets camelCased to "partNumber" without my consent
        // groovy continues to find new and fun ways to make me want to unalive myself
        def gen = new JsonGenerator.Options()
            .addConverter(new JsonGenerator.Converter() {
                @Override
                boolean handles(Class<?> aClass) {
                    return CompletedPart.isAssignableFrom(aClass)
                }

                @Override
                Object convert(Object o, String s) {
                    if (o instanceof CompletedPart)
                        return ["ETag": o.ETag, "PartNumber": o.PartNumber]

                    return null
                }
            }).build()

        def endUploadBody = gen.toJson([
            "path": this.toUri().toString(),
            "upload_id": uploadId,
            "parts": parts,
        ])

        request = HttpRequest.newBuilder()
            .uri(URI.create("${host}/ldata/end-upload"))
            .header("Content-Type", "application/json")
            .header("Authorization", LatchPathUtils.getAuthHeader())
            .POST(HttpRequest.BodyPublishers.ofString(endUploadBody))
            .build()

        client.send(request)
    }

    URL getSignedURL() {
        JsonBuilder builder = new JsonBuilder()
        builder(["path": this.toUri().toString()])

        HttpRetryClient client = new HttpRetryClient()
        def request =  HttpRequest.newBuilder()
            .uri(URI.create("${host}/ldata/get-signed-url"))
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
}
