package nextflow.file.http

import java.nio.file.Path
import java.nio.file.Paths

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
        return new URI("latch://${fs.domain}${path}")
    }
}
