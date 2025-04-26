package nextflow.forch

import java.nio.file.Path
import java.util.concurrent.TimeUnit

import groovy.json.JsonBuilder
import groovy.util.logging.Slf4j
import nextflow.exception.ProcessException
import nextflow.exception.ProcessUnrecoverableException
import nextflow.executor.BashWrapperBuilder
import nextflow.executor.res.AcceleratorResource
import nextflow.file.FileHelper
import nextflow.processor.TaskHandler
import nextflow.processor.TaskRun
import nextflow.processor.TaskStatus
import nextflow.script.ProcessConfig
import nextflow.util.Escape
import nextflow.util.MemoryUnit

@Slf4j
class ForchTaskHandler extends TaskHandler {

    ProcessConfig processConfig

    Integer forchTaskId

    Path remoteBinDir = null


    ForchTaskHandler(TaskRun task, Path remoteBinDir) {
        super(task)

        this.processConfig = task.processor.config
        this.remoteBinDir = remoteBinDir
    }

    private String subprocess(String command) {
        StringBuilder stdout = new StringBuilder(), stderr = new StringBuilder();
        Process proc = command.execute()

        proc.consumeProcessOutput(stdout, stderr)
        proc.waitFor(5, TimeUnit.SECONDS)

        return stdout.toString().trim()
    }

    private String getCurrentStatus() {
        if (this.forchTaskId == null) return

        return subprocess("forch status ${forchTaskId}")
    }

    @Override
    boolean checkIfRunning() {
        def running =  this.currentStatus == 'running'
        if (running)
            status = TaskStatus.RUNNING
        return running
    }

    @Override
    boolean checkIfCompleted() {
        def cur = this.currentStatus
        if (cur != "succeeded" && cur != "failed") return false

        // todo(ayush): single query
        def exitStatus = subprocess("forch exitcode ${forchTaskId}")
        task.exitStatus = Integer.parseInt(exitStatus)

        // todo(ayush): logs, retries
        task.stdout = ""
        task.stderr = ""
        status = TaskStatus.COMPLETED
        return true
    }

    @Override
    void kill() {
        // noop
    }

    @Override
    void prepareLauncher() {
        new ForchTaskWrapperBuilder(this.task.toTaskBean()).build()
    }

    @Override
    void submit() {
        JsonBuilder builder = new JsonBuilder()

        int cpus = task.config.getCpus()
        MemoryUnit memory = task.config.getMemory() ?: MemoryUnit.of("2GiB")

        // todo(ayush): gpu support
        // AcceleratorResource acc = task.config.getAccelerator()

        String cmd = """\
            trap "{ ret=\$?; s5cmd cp ${TaskRun.CMD_LOG} ${task.workDir.toUriString()}/${TaskRun.CMD_LOG}||true; exit \$ret; }" EXIT; 
            s5cmd --no-verify-ssl cat ${task.workDir.toUriString()}/${TaskRun.CMD_RUN} | bash 2>&1 | tee ${TaskRun.CMD_LOG}
        """.stripIndent().trim()

        if (remoteBinDir != null) {
            cmd = """\
                s5cmd --no-verify-ssl cp s3:/${remoteBinDir}/* /nextflow-bin
                chmod +x /nextflow-bin/* || true
                export PATH=/nextflow-bin:\$PATH
                
            """ + cmd
        }

        builder([
            "display_name": this.task.name,
            "container_image": this.task.container,
            "container_entrypoint": [
                "/bin/bash",
                "-c",
                cmd,
            ],
            "cpus": cpus,
            "memory_bytes": memory.bytes,
            "gpu_type": null,
            "gpus": 0,
        ])

        List<String> command = ["forch", "create", builder.toString()]
        StringBuilder stdout = new StringBuilder(), stderr = new StringBuilder();
        Process proc = command.execute()

        proc.consumeProcessOutput(stdout, stderr)
        proc.waitFor(5, TimeUnit.SECONDS)

        log.debug("${task.name} taskExecutionId: $stdout, err: $stderr")

        this.forchTaskId = Integer.parseInt(stdout.toString().trim())
    }
}
