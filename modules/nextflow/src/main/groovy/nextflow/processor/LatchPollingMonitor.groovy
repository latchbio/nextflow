package nextflow.processor

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.util.Duration
import nextflow.util.MemoryUnit


@Slf4j
@CompileStatic
class LatchPollingMonitor extends LocalPollingMonitor {

    // default CPUs to request for a task if no value specified
    private int cpuDefault;

    // default memory (in Bytes) to request for a task if no value specified
    private long memoryDefault;

    protected LatchPollingMonitor(Map params) {
        super(params)
        this.cpuDefault = params.cpuDefault as int
        this.memoryDefault = params.memoryDefault as long
        assert cpuDefault>0, "Default `cpuDefault` attribute cannot be zero"
        assert memoryDefault>0, "Default `memoryDefault` attribute cannot zero"
    }


    static LatchPollingMonitor create(
        Session session,
        String name,
        int defQueueSize,
        Duration defPollInterval,
        int cpuLimit,
        long memoryLimit,
        int cpuDefault,
        long memoryDefault
    ) {
        assert session
        assert name
        final capacity = session.getQueueSize(name, defQueueSize)
        final pollInterval = session.getPollInterval(name, defPollInterval)
        final dumpInterval = session.getMonitorDumpInterval(name)

        log.debug "Creating latch task monitor for executor '$name' > cpuLimit=$cpuLimit; memoryLimit=$memoryLimit; cpuDefault=$cpuDefault; memoryDefault=$memoryDefault; capacity=$capacity; pollInterval=$pollInterval; dumpInterval=$dumpInterval"

        new LatchPollingMonitor(
            name: name,
            session: session,
            capacity: capacity,
            pollInterval: pollInterval,
            dumpInterval: dumpInterval,
            cpus: cpuLimit,
            memory: memoryLimit,
            cpuDefault: cpuDefault,
            memoryDefault: memoryDefault,

        )
    }

    @Override
    protected int cpus(TaskHandler handler) {
        def conf = handler.task.getConfig()
        if (conf == null || !conf.hasCpus())
            return cpuDefault

        return conf.getCpus()
    }

    @Override
    protected long mem(TaskHandler handler) {
        def conf = handler.task.getConfig()
        if (conf == null || conf.getMemory() == null)
            return memoryDefault

        return conf.getMemory().toBytes()
    }
}
