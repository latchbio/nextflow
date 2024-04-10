package nextflow.file.http

import java.nio.file.Path

import groovy.util.logging.Slf4j

@Slf4j
class LatchFileSystem extends XFileSystem {
    XFileSystemProvider provider
    String domain

    LatchFileSystem(XFileSystemProvider provider, URI base) {
        super(provider, base)

        this.provider = provider
        this.domain = base.authority ?: ""
    }

    @Override
    Path getPath(String first, String... more) {
        return new LatchPath(this, first, more)
    }
}
