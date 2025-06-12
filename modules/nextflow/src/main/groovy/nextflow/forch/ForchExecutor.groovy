package nextflow.forch

import java.nio.file.Path

import groovy.util.logging.Slf4j
import nextflow.executor.Executor
import nextflow.extension.FilesEx
import nextflow.file.FileHelper
import nextflow.processor.TaskHandler
import nextflow.processor.TaskMonitor
import nextflow.processor.TaskPollingMonitor
import nextflow.processor.TaskRun
import nextflow.util.DispatcherClient
import nextflow.util.ForchClient
import nextflow.util.Duration

@Slf4j
class ForchExecutor extends Executor {

    Path remoteBinDir = null
    private ForchClient forchClient

    @Override
    protected TaskMonitor createTaskMonitor() {
        return TaskPollingMonitor.create(session, name, 100, Duration.of("15s"))
    }

    @Override
    protected void register() {
        // todo(ayush): decouple dispatcher and executor
        this.dispatcherClient = new DispatcherClient()
        this.forchClient = new ForchClient()
        uploadBinDir()
    }

    @Override
    TaskHandler createTaskHandler(TaskRun task) {
        return new ForchTaskHandler(task, this.forchClient, remoteBinDir)
    }

    protected void uploadBinDir() {
        if( session.binDir && !session.binDir.empty() ) {
            def s3 = getTempDir()
            log.info "Uploading local `bin` scripts folder to ${s3.toUriString()}/bin"
            remoteBinDir = FilesEx.copyTo(session.binDir, s3)
        }
    }
}
