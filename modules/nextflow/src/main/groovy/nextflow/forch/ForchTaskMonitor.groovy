package nextflow.forch

import groovy.util.logging.Slf4j
import nextflow.processor.TaskHandler
import nextflow.processor.TaskMonitor

@Slf4j
class ForchTaskMonitor implements TaskMonitor {
    @Override
    void schedule(TaskHandler handler) {

    }

    @Override
    boolean evict(TaskHandler handler) {
        return false
    }

    @Override
    TaskMonitor start() {
        return null
    }

    @Override
    void signal() {
        // noop
    }
}
