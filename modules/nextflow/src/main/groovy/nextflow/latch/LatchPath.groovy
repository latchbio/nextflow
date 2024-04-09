package nextflow.latch

import java.nio.file.FileSystem
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.WatchEvent
import java.nio.file.WatchKey
import java.nio.file.WatchService
import java.util.function.Consumer

class LatchPath implements Path {
    LatchFileSystem fs

    @Override
    FileSystem getFileSystem() {
        return fs
    }

    @Override
    boolean isAbsolute() {
        return false
    }

    @Override
    Path getRoot() {
        return null
    }

    @Override
    Path getFileName() {
        return null
    }

    @Override
    Path getParent() {
        return null
    }

    @Override
    int getNameCount() {
        return 0
    }

    @Override
    Path getName(int index) {
        return null
    }

    @Override
    Path subpath(int beginIndex, int endIndex) {
        return null
    }

    @Override
    boolean startsWith(Path other) {
        return false
    }

    @Override
    boolean startsWith(String other) {
        return super.startsWith(other)
    }

    @Override
    boolean endsWith(Path other) {
        return false
    }

    @Override
    boolean endsWith(String other) {
        return super.endsWith(other)
    }

    @Override
    Path normalize() {
        return null
    }

    @Override
    Path resolve(Path other) {
        return null
    }

    @Override
    Path resolve(String other) {
        return super.resolve(other)
    }

    @Override
    Path resolveSibling(Path other) {
        return super.resolveSibling(other)
    }

    @Override
    Path resolveSibling(String other) {
        return super.resolveSibling(other)
    }

    @Override
    Path relativize(Path other) {
        return null
    }

    @Override
    URI toUri() {
        return null
    }

    @Override
    Path toAbsolutePath() {
        return null
    }

    @Override
    Path toRealPath(LinkOption... options) throws IOException {
        return null
    }

    @Override
    File toFile() {
        return super.toFile()
    }

    @Override
    WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers) throws IOException {
        return null
    }

    @Override
    WatchKey register(WatchService watcher, WatchEvent.Kind<?>... events) throws IOException {
        return super.register(watcher, events)
    }

    @Override
    Iterator<Path> iterator() {
        return super.iterator()
    }

    @Override
    void forEach(Consumer<? super Path> action) {
        super.forEach(action)
    }

    @Override
    Spliterator<Path> spliterator() {
        return super.spliterator()
    }

    @Override
    int compareTo(Path other) {
        return 0
    }
}
