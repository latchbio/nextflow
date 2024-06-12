package nextflow.file.http

import java.nio.file.Path

import groovy.util.logging.Slf4j

import java.util.concurrent.ForkJoinPool

@Slf4j
class LatchFileSystem extends XFileSystem {
    LatchFileSystemProvider provider
    String domain

    LatchFileSystem(LatchFileSystemProvider provider, String domain) {
        super(provider, new URI("latch", domain, "/", null, null))

        this.provider = provider
        this.domain = domain
    }

    @Override
    Path getPath(String first, String... more) {
        return new LatchPath(this, first, more)
    }
}
