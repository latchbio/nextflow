package nextflow.file.http

import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.channels.SeekableByteChannel
import java.nio.file.AccessDeniedException
import java.nio.file.AccessMode
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.CopyOption
import java.nio.file.DirectoryNotEmptyException
import java.nio.file.DirectoryStream
import java.nio.file.FileAlreadyExistsException
import java.nio.file.FileStore
import java.nio.file.FileSystem
import java.nio.file.FileSystemAlreadyExistsException
import java.nio.file.FileSystemNotFoundException
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.NoSuchFileException
import java.nio.file.NotDirectoryException
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.ProviderMismatchException
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileAttribute
import java.nio.file.attribute.FileAttributeView
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

@CompileStatic
@Slf4j
class LatchFileSystemProvider extends XFileSystemProvider {

    private GQLClient client = new GQLClient()

    /**
     * Map of domain -> filesystem
     * keys are e.g. 1721.account, bucket.mount, etc.
     */
    private final Map<String, LatchFileSystem> fileSystems = new HashMap<String, LatchFileSystem>();

    @Override
    String getScheme() {
        return "latch"
    }

    @Override
    FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
        String domain = LatchPathUtils.getDomain(uri)
        if (fileSystems.containsKey(domain))
            throw new FileSystemAlreadyExistsException()

        LatchFileSystem fs = new LatchFileSystem(this, domain)
        fileSystems[domain] = fs
        return fs
    }

    @Override
    FileSystem getFileSystem(URI uri) {
        String domain = LatchPathUtils.getDomain(uri)

        if (!this.fileSystems.containsKey(domain)) {
            throw new FileSystemNotFoundException("S3 filesystem not yet created. Use newFileSystem() instead");
        }

        return this.fileSystems[domain];
    }

    @Override
    FileSystem getFileSystem(URI uri, boolean canCreate) {
        return getFileSystem(uri)
    }

    @Override
    Path getPath(URI uri) {
        return getFileSystem(uri).getPath(uri.path)
    }

    @Override
    SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        if (!(path instanceof LatchPath))
            throw new ProviderMismatchException()

        def lp = (LatchPath) path

        if (options.size() > 0) {
            for (OpenOption opt: options) {
                // All OpenOption values except for APPEND and WRITE are allowed
                if (opt == StandardOpenOption.APPEND) {
                    throw new UnsupportedOperationException("'$opt' not allowed");
                } else if (opt == StandardOpenOption.WRITE) {
                    return getWriteChannel(path)
                }
            }
        }

        Path url = Paths.get(lp.getSignedURL().toURI())
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
            }
        }
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
        return null
    }

    @Override
    void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {}

    @Override
    void delete(Path path) throws IOException {}

    @Override
    void copy(Path source, Path target, CopyOption... options) throws IOException {}

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
        for( AccessMode m : modes ) {
            if( m == AccessMode.EXECUTE )
                throw new AccessDeniedException("Execute mode not supported")
        }
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
                            type
                        }
                    }
                }
            }
        """, ["argPath": path.toUriString()])["ldataResolvePathToNode"]

        if (res["path"] != null) {
            return (A) new LatchFileAttributes()
        }

        return (A) new LatchFileAttributes((String) res["ldataNode"]["finalLinkTarget"]["type"])
    }

    @Override
    Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
        return null
    }

    @Override
    void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {}
}
