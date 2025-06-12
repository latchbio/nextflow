package nextflow.forch

import java.nio.file.Path
import java.util.concurrent.TimeUnit

import groovy.json.JsonBuilder
import groovy.util.logging.Slf4j
import nextflow.Session
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

    Session session

    ForchTaskHandler(TaskRun task, Path remoteBinDir, Session session) {
        super(task)

        this.processConfig = task.processor.config
        this.remoteBinDir = remoteBinDir
        this.session = session
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

        def serverIp = System.getenv("latch_internal_nfs_server_ip")

        String cmd = """\
            if [[ "\$(command -v apt-get)" ]]; then
                apt-get update
                apt-get install -y nfs-common
            elif [[ "\$(command -v yum)" ]]; then
                yum install -y nfs-utils
            elif [[ "\$(command -v dnf)" ]]; then
                dnf install -y nfs-utils
            fi

            mkdir --parents ${session.baseDir}
        
            until mount -t nfs4 [${serverIp}]:/ ${session.baseDir}
            do
                echo "failed to mount nfs share: retrying..."
                sleep 5 
            done
            
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
