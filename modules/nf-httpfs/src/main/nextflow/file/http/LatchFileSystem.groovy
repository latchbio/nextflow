package nextflow.file.http

import java.nio.file.Path

import groovy.util.logging.Slf4j

import java.util.concurrent.ForkJoinPool

@Slf4j
class LatchFileSystem extends XFileSystem {
    LatchFileSystemProvider provider
    String domain
    ForkJoinPool executor

    LatchFileSystem(LatchFileSystemProvider provider, String domain) {
        super(provider, new URI("latch", domain, "/", null, null))

        this.provider = provider
        this.domain = domain

        this.executor = new ForkJoinPool(
            Math.max(10, Runtime.getRuntime().availableProcessors()*3),
            ForkJoinPool.defaultForkJoinWorkerThreadFactory,
            null,
            true // async
        )
    }

    @Override
    Path getPath(String first, String... more) {
        return new LatchPath(this, first, more)
    }
}
