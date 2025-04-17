package nextflow.forch

import groovy.util.logging.Slf4j
import nextflow.executor.Executor
import nextflow.processor.TaskHandler
import nextflow.processor.TaskMonitor
import nextflow.processor.TaskPollingMonitor
import nextflow.processor.TaskRun
import nextflow.util.DispatcherClient
import nextflow.util.Duration

@Slf4j
class ForchExecutor extends Executor {

    @Override
    protected TaskMonitor createTaskMonitor() {
        return TaskPollingMonitor.create(session, name, 100, Duration.of("15s"))
    }

    @Override
    protected void register() {
        // todo(ayush): decouple dispatcher and executor
        this.dispatcherClient = new DispatcherClient()
        this.dispatcherClient.debug = true
    }

    @Override
    TaskHandler createTaskHandler(TaskRun task) {
        return new ForchTaskHandler(task)
    }
}
