package nextflow.forch

import nextflow.util.DispatcherClient
import nextflow.util.ForchClient

import java.nio.file.Path

import groovy.util.logging.Slf4j

import nextflow.Session

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
    private DispatcherClient dispatcherClient
    Session session

    ForchTaskHandler(TaskRun task, Path remoteBinDir, Session session, ForchClient forchClient, DispatcherClient dispatcherClient) {
        super(task)

        this.processConfig = task.processor.config
        this.remoteBinDir = remoteBinDir
        this.forchClient = forchClient
        this.dispatcherClient = dispatcherClient

        this.session = session
    }

    private String getCurrentStatus() {
        if (this.forchTaskId == null) return

        return this.forchClient.getTaskStatus(this.forchTaskId)
    }

    @Override
    boolean checkIfRunning() {
        def running =  this.currentStatus == 'RUNNING'
        if (running)
            status = TaskStatus.RUNNING
        return running
    }

    @Override
    boolean checkIfCompleted() {
        def cur = this.currentStatus
        if (cur != "SUCCEEDED" && cur != "FAILED") return false

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
        forchClient.abortTasks([forchTaskId])
    }

    @Override
    void prepareLauncher() {
        new ForchTaskWrapperBuilder(this.task.toTaskBean()).build()
    }

    @Override
    void submit() {
        int cpus = task.config.getCpus()
        MemoryUnit memory = task.config.getMemory() ?: MemoryUnit.of("2GiB")

        final containerOpts = task.config.getContainerOptionsMap()

        MemoryUnit shm = null;
        if (containerOpts != null && containerOpts.exists("shm-size")) {
            shm = new MemoryUnit(containerOpts.getFirstValue("shm-size") as String)
        }

        // todo(ayush): gpu support
        // AcceleratorResource acc = task.config.getAccelerator()

        def serverIp = System.getenv("latch_internal_nfs_server_ip")
        if (serverIp == null)
            throw new RuntimeException("failed to get server ip")

        String cmd = """\
            mkdir --parents ${session.baseDir}

            chown -R root:root /usr/bin/mount 2>&1 > /dev/null

            until mount -t nfs4 [${serverIp}]:/ ${session.baseDir} 2>&1 > /dev/null
            do
                sleep 5
            done

            cat ${task.workDir}/${TaskRun.CMD_RUN} | bash 2>&1
        """.stripIndent().trim()

        if (remoteBinDir != null) {
            cmd = """\
                mkdir -p /nextflow-bin
                cp ${remoteBinDir}/* /nextflow-bin
                chmod +x /nextflow-bin/*
                export PATH=/nextflow-bin:\$PATH
            """.stripIndent() + cmd
        }

        List<String> entrypoint = [
            "/bin/bash",
            "-c",
            cmd,
        ]

        this.forchTaskId = this.forchClient.submitTask(
            this.task.name,
            this.task.container,
            entrypoint,
            cpus,
            memory.bytes,
            shm?.bytes ?: 0
        )

        // todo(rahul): put this in a single transaction with submitTask
        this.dispatcherClient.updateForchTaskId(
            this.taskExecutionId,
            this.forchTaskId
        )
    }
}
