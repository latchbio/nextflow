package nextflow.forch

import java.util.concurrent.TimeUnit

import groovy.json.JsonBuilder
import groovy.util.logging.Slf4j
import nextflow.executor.BashWrapperBuilder
import nextflow.executor.res.AcceleratorResource
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


    ForchTaskHandler(TaskRun task) {
        super(task)

        this.processConfig = task.processor.config
    }

    private String getCurrentStatus() {
        if (this.forchTaskId == null) return

        String command = "forch status ${forchTaskId}"
        StringBuilder stdout = new StringBuilder(), stderr = new StringBuilder();
        Process proc = command.execute()

        proc.consumeProcessOutput(stdout, stderr)
        proc.waitFor(5, TimeUnit.SECONDS)

        return stdout.toString().trim()
    }

    @Override
    boolean checkIfRunning() {
        return this.currentStatus == 'running'
    }

    @Override
    boolean checkIfCompleted() {
        return this.currentStatus == 'succeeded' || this.currentStatus == 'failed'
    }

    @Override
    void kill() {
        // noop
    }

    @Override
    void submit() {
        JsonBuilder builder = new JsonBuilder()

        int cpus = task.config.getCpus()
        MemoryUnit memory = task.config.getMemory() ?: MemoryUnit.of("2GiB")

        // todo(ayush): gpu support
        // AcceleratorResource acc = task.config.getAccelerator()

        builder([
            "display_name": this.task.name,
            "container_image": this.task.container,
            "container_entrypoint": [
                "/bin/bash",
                "-ue",
                "${Escape.path(task.workDir)}/${TaskRun.CMD_RUN}"
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
