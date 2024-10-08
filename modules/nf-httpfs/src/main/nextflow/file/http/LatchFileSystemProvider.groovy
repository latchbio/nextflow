package nextflow.file.http

import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.channels.SeekableByteChannel
import java.nio.file.AccessDeniedException
import java.nio.file.AccessMode
import java.nio.file.CopyOption
import java.nio.file.DirectoryStream
import java.nio.file.FileStore
import java.nio.file.FileSystem
import java.nio.file.FileSystemAlreadyExistsException
import java.nio.file.FileSystemNotFoundException
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.NoSuchFileException
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.ProviderMismatchException
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileAttribute
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

@CompileStatic
@Slf4j
class LatchFileSystemProvider extends XFileSystemProvider {

    private GQLClient client = new GQLClient()

    /**
     * Map of domain -> filesystem
     * keys are e.g. 1721.account, bucket.mount, etc.
     */
    private final Map<String, LatchFileSystem> fileSystems = new HashMap<String, LatchFileSystem>()

    static ExecutorService uploadExecutor = Executors.newFixedThreadPool(20)
    static ExecutorService downloadExecutor = Executors.newFixedThreadPool(20)

    static void shutdown() {
        uploadExecutor.shutdown()
        downloadExecutor.shutdown()
    }

    @Override
    String getScheme() {
        return "latch"
    }

    @Override
    FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
        String domain = LatchPathUtils.getDomain(uri)
        if (fileSystems.containsKey(domain))
            throw new FileSystemAlreadyExistsException()

        return new LatchFileSystem(this, domain)
    }

    @Override
    FileSystem getFileSystem(URI uri) {
        return getFileSystem(uri, false)
    }

    @Override
    FileSystem getFileSystem(URI uri, boolean canCreate) {
        String domain = LatchPathUtils.getDomain(uri)

        if (!canCreate) {
            if (!this.fileSystems.containsKey(domain)) {
                throw new FileSystemNotFoundException("Latch filesystem not yet created. Use newFileSystem() instead");
            }

            return this.fileSystems[domain]
        }

        synchronized (this.fileSystems) {
            FileSystem result = this.fileSystems[domain]
            if( result == null ) {
                result = newFileSystem(uri, [:])
                this.fileSystems[domain] = (LatchFileSystem) result
            }

            return result
        }
    }

    @Override
    Path getPath(URI uri) {
        return getFileSystem(uri, true).getPath(uri.path)
    }

    @Override
    SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        if (!(path instanceof LatchPath))
            throw new ProviderMismatchException()

        def lp = (LatchPath) path
        if (lp.exists()) {
            if (!readAttributes(lp, LatchFileAttributes).isRegularFile())
                throw new Exception("Cannot create input byte channel because ${lp.toUriString()} is a directory.")
        }

        if (options.size() > 0) {
            for (OpenOption opt: options) {
                if (opt == StandardOpenOption.APPEND) {
                    throw new UnsupportedOperationException("'$opt' not allowed");
                } else if (opt == StandardOpenOption.WRITE) {
                    return getWriteChannel(path)
                }
            }
        }

        Path url = Paths.get(lp.getSignedURL().toURI())
        log.debug "Downloading ${lp.toUriString()}"
        return url.fileSystem.provider().newByteChannel(url, options, attrs)
    }

    SeekableByteChannel getWriteChannel(Path path) {
        if (!(path instanceof LatchPath))
            throw new ProviderMismatchException()

        def lp = (LatchPath) path
        def temp = File.createTempFile("latch", ".tmp")
        def writer = temp.newOutputStream()

        return new SeekableByteChannel() {

            private long position = 0

            @Override
            int read(ByteBuffer buffer) throws IOException {
                throw new UnsupportedOperationException("Read operation not supported")
            }

            @Override
            int write(ByteBuffer src) throws IOException {
                while (src.hasRemaining()) {
                    writer.write(src.get())
                }
                return src.limit()
            }

            @Override
            long position() throws IOException {
                return position
            }

            @Override
            SeekableByteChannel position(long newPosition) throws IOException {
                throw new UnsupportedOperationException("Position operation not supported")
            }

            @Override
            long size() throws IOException {
                return temp.size()
            }

            @Override
            SeekableByteChannel truncate(long unused) throws IOException {
                throw new UnsupportedOperationException("Truncate operation not supported")
            }

            @Override
            boolean isOpen() {
                return true
            }

            @Override
            void close() throws IOException {
                writer.close()
                lp.upload(Paths.get(temp.getPath()))
                temp.delete()
            }
        }
    }

    @Override
    InputStream newInputStream(Path path, OpenOption... options) throws IOException {
        Set<OpenOption> opts = new HashSet<OpenOption>()
        for (OpenOption opt: options) {
            opts.add(opt)
        }

        def chan = newByteChannel(path, opts)
        return Channels.newInputStream(chan)
    }

    @Override
    OutputStream newOutputStream(Path path, OpenOption... options) throws IOException {
        Set<OpenOption> opts = new HashSet<OpenOption>()
        for (OpenOption opt: options) {
            opts.add(opt)
        }

        def chan = newByteChannel(path, opts)
        return Channels.newOutputStream(chan)
    }

    @Override
    DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
        if (!(dir instanceof LatchPath))
            throw new ProviderMismatchException()

        def lp = (LatchPath) dir
        def res = client.execute("""
            query LDataChildren(\$argPath: String!) {
                ldataResolvePathData(argPath: \$argPath) {
                    finalLinkTarget {
                        childLdataTreeEdges(filter: { child: { removed: { equalTo: false } } }) {
                            nodes {
                                child {
                                    name
                                    type
                                }
                            }
                        }
                    }
                }
            }
        """, ["argPath": lp.toUriString()])["ldataResolvePathData"]

        if (res == null) {
            throw new Exception("Cannot iterate over children: ${lp.toUriString()} does not exist")
        }


        return new DirectoryStream<Path>() {
            @Override
            Iterator<Path> iterator() {
                List<Path> paths = (res["finalLinkTarget"]["childLdataTreeEdges"]["nodes"] as List<Map>)
                    .collect({it -> return dir.resolve(it["child"]["name"] as String)})

                return paths.iterator()
            }

            @Override
            void close() throws IOException {}
        }
    }

    @Override
    void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {}

    @Override
    void delete(Path path) throws IOException {
        if (!(path instanceof LatchPath))
            throw new ProviderMismatchException()

        LatchPath lp = (LatchPath) path
        LatchFileAttributes attrs = readAttributes(lp, LatchFileAttributes)

        client.execute("""
             mutation RemoveNode(\$argNodeId: BigInt!) {
                ldataRmr(input: {argNodeId: \$argNodeId}) {
                    clientMutationId
                }
            }
            """,
            ["argNodeId": attrs.nodeId]
        )
    }

    @Override
    void copy(Path source, Path target, CopyOption... options) throws IOException {
        // todo(ayush): better errors for common failure modes
        // todo(ayush): deal with copy options
        if (target.scheme == "latch" && source.scheme == "file") {
            LatchPath lp = (LatchPath) target
            lp.upload(source)
            return
        }

        if (source.scheme == "latch" && target.scheme == "file") {
            LatchPath lp = (LatchPath) source
            lp.download(target)
            return
        }

        throw new RuntimeException("Copy failed: either source or target must be Latch path")
    }

    @Override
    void move(Path source, Path target, CopyOption... options) throws IOException {}

    @Override
    boolean isSameFile(Path path, Path path2) throws IOException {
        return false
    }

    @Override
    boolean isHidden(Path path) throws IOException {
        return false
    }

    @Override
    FileStore getFileStore(Path path) throws IOException {
        return null
    }

    @Override
    void checkAccess(Path path, AccessMode... modes) throws IOException {
        if (!(path instanceof LatchPath)) {
            log.info "Invalid path type: ${path.getClass()}, ${path.fileName}"
            throw new ProviderMismatchException()
        }


        for (AccessMode m : modes) {
            if (m == AccessMode.EXECUTE)
                throw new AccessDeniedException("Execute mode not supported")
        }

        def lp = (LatchPath) path
        if (!lp.exists())
            throw new NoSuchFileException("Path ${lp.toUriString()} does not exist")
    }

    @Override
    def <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
        if (!type.isAssignableFrom(LatchFileAttributes.class)) {
            throw new UnsupportedOperationException("Not a valid ${getScheme().toUpperCase()} file attribute type: $type")
        }

        Map res = client.execute("""
            query GetFileAttrs(\$argPath: String!) {
                ldataResolvePathToNode(path: \$argPath) {
                    path
                    ldataNode {
                        finalLinkTarget {
                            id
                            type
                            ldataObjectMeta {
                                contentSize
                            }
                        }
                    }
                }
            }
        """, ["argPath": path.toUriString()])["ldataResolvePathToNode"]

        if (res["path"] != null)
            throw new NoSuchFileException("Path ${path.toUriString()} does not exist")

        Map flt = res["ldataNode"]["finalLinkTarget"] as Map

        long size = 0
        if (
            flt["type"] == "OBJ"
            && flt["ldataObjectMeta"] != null
            && flt["ldataObjectMeta"]["contentSize"] != null
        ) {
            size = Long.parseLong(flt["ldataObjectMeta"]["contentSize"] as String)
        }

        long nodeId = Long.parseLong(flt["id"] as String)

        return (A) new LatchFileAttributes(nodeId, (String) flt["type"], size)
    }

    @Override
    Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
        return null
    }

    @Override
    void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {}
}
