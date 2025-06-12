package nextflow.forch

import nextflow.util.ForchClient

import java.nio.file.Path

import groovy.util.logging.Slf4j
import nextflow.processor.TaskHandler
import nextflow.processor.TaskRun
import nextflow.processor.TaskStatus
import nextflow.script.ProcessConfig
import nextflow.util.MemoryUnit

@Slf4j
class ForchTaskHandler extends TaskHandler {

    ProcessConfig processConfig

    Integer forchTaskId

    Path remoteBinDir = null

    private ForchClient forchClient

    ForchTaskHandler(TaskRun task, ForchClient client, Path remoteBinDir) {
        super(task)

        this.processConfig = task.processor.config
        this.remoteBinDir = remoteBinDir
        this.forchClient = client
    }

    private String getCurrentStatus() {
        if (this.forchTaskId == null) return

        return this.forchClient.getTaskStatus(this.forchTaskId)
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
        task.exitStatus = this.forchClient.getTaskExitCode(this.forchTaskId)

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
        int cpus = task.config.getCpus()
        MemoryUnit memory = task.config.getMemory() ?: MemoryUnit.of("2GiB")

        // todo(ayush): gpu support
        // AcceleratorResource acc = task.config.getAccelerator()

        String cmd = """\
            trap "{ ret=\$?; cp ${TaskRun.CMD_LOG} ${task.workDir}/${TaskRun.CMD_LOG}||true; exit \$ret; }" EXIT; 
            cat ${task.workDir}/${TaskRun.CMD_RUN} | bash 2>&1 | tee ${TaskRun.CMD_LOG}
        """.stripIndent().trim()

        if (remoteBinDir != null) {
            cmd = """\
                mkdir -p /nextflow-bin
                cp ${remoteBinDir}/* /nextflow-bin
                chmod +x /nextflow-bin/*
                export PATH=/nextflow-bin:\$PATH
                
            """.stripIndent() + cmd
        }

        this.forchTaskId = this.forchClient.submitTask(
            this.task.name,
            this.task.container,
            [
                "/bin/bash",
                "-c",
                cmd,
            ],
            cpus,
            memory.bytes
        )
    }
}
