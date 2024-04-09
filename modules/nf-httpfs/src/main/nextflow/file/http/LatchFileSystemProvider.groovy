package nextflow.file.http

import java.nio.file.AccessMode
import java.nio.file.Path

import groovy.transform.CompileStatic


@CompileStatic
class LatchFileSystemProvider extends XFileSystemProvider {
    @Override
    String getScheme() {
        return "latch"
    }

    @Override
    void checkAccess(Path path, AccessMode... modes) throws IOException {} // superhack
}
