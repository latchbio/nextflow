package nextflow.latch

import java.nio.file.FileStore
import java.nio.file.FileSystem
import java.nio.file.Path
import java.nio.file.PathMatcher
import java.nio.file.WatchService
import java.nio.file.attribute.UserPrincipalLookupService
import java.nio.file.spi.FileSystemProvider

import org.apache.commons.lang.NotImplementedException

class LatchFileSystem extends FileSystem {
    public LatchFileSystemProvider fsp

    LatchFileSystem(LatchFileSystemProvider fsp) {
        this.fsp = fsp
    }

    @Override
    FileSystemProvider provider() {
        return fsp
    }

    @Override
    void close() throws IOException {
        throw new NotImplementedException()
    }

    @Override
    boolean isOpen() {
        throw new NotImplementedException()
    }

    @Override
    boolean isReadOnly() {
        throw new NotImplementedException()
    }

    @Override
    String getSeparator() {
        return "/"
    }

    @Override
    Iterable<Path> getRootDirectories() {
        throw new NotImplementedException()
    }

    @Override
    Iterable<FileStore> getFileStores() {
        throw new NotImplementedException()
    }

    @Override
    Set<String> supportedFileAttributeViews() {
        throw new NotImplementedException()
    }

    @Override
    Path getPath(String first, String... more) {
        return Path.of(new URI("latch://"))
    }

    @Override
    PathMatcher getPathMatcher(String syntaxAndPattern) {
        return null
    }

    @Override
    UserPrincipalLookupService getUserPrincipalLookupService() {
        return null
    }

    @Override
    WatchService newWatchService() throws IOException {
        return null
    }
}
