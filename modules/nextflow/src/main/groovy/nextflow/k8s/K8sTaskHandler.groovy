/*
 * Copyright 2013-2023, Seqera Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nextflow.k8s

import nextflow.k8s.client.K8sResponseException
import nextflow.k8s.client.PodUnschedulableException
import nextflow.util.DispatcherClient

import java.nio.file.Path

import groovy.transform.CompileDynamic
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import nextflow.SysEnv
import nextflow.container.DockerBuilder
import nextflow.exception.ProcessSubmitException
import nextflow.executor.BashWrapperBuilder
import nextflow.fusion.FusionAwareTask
import nextflow.k8s.client.K8sClient
import nextflow.k8s.model.PodEnv
import nextflow.k8s.model.PodOptions
import nextflow.k8s.model.PodSpecBuilder
import nextflow.k8s.model.ResourceType
import nextflow.processor.TaskHandler
import nextflow.processor.TaskRun
import nextflow.processor.TaskStatus
import nextflow.util.Escape
import nextflow.util.PathTrie
/**
 * Implements the {@link TaskHandler} interface for Kubernetes pods
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
class K8sTaskHandler extends TaskHandler implements FusionAwareTask {

    @Lazy
    static private final String OWNER = {
        if( System.getenv('NXF_OWNER') ) {
            return System.getenv('NXF_OWNER')
        }
        else {
            def p = ['bash','-c','echo -n $(id -u):$(id -g)'].execute();
            p.waitFor()
            return p.text
        }

    } ()

    private ResourceType resourceType = ResourceType.Pod

    private K8sClient client

    private DispatcherClient dispatcherClient

    private BashWrapperBuilder builder

    private Path outputFile

    private Path errorFile

    private Path exitFile

    private K8sExecutor executor

    K8sTaskHandler( TaskRun task, K8sExecutor executor ) {
        super(task)

        this.executor = executor
        this.client = executor.client
        this.dispatcherClient = executor.dispatcherClient
        this.outputFile = task.workDir.resolve(TaskRun.CMD_OUTFILE)
        this.errorFile = task.workDir.resolve(TaskRun.CMD_ERRFILE)
        this.exitFile = task.workDir.resolve(TaskRun.CMD_EXIT)
        this.resourceType = executor.k8sConfig.useJobResource() ? ResourceType.Job : ResourceType.Pod
    }

    /** only for testing -- do not use */
    protected K8sTaskHandler() {

    }

    /**
     * @return The workflow execution unique run name
     */
    protected String getRunName() {
        executor.session.runName
    }

    protected K8sConfig getK8sConfig() { executor.getK8sConfig() }

    protected boolean useJobResource() { resourceType==ResourceType.Job }

    protected List<String> getContainerMounts() {

        if( !k8sConfig.getAutoMountHostPaths() ) {
            return Collections.<String>emptyList()
        }

        // get input files paths
        final paths = DockerBuilder.inputFilesToPaths(builder.getInputFiles())
        final binDirs = builder.binDirs
        final workDir = builder.workDir
        // add standard paths
        if( binDirs )
            paths.addAll(binDirs)
        if( workDir )
            paths << workDir

        def trie = new PathTrie()
        paths.each { trie.add(it) }

        // defines the mounts
        trie.longest()
    }

    protected BashWrapperBuilder createBashWrapper(TaskRun task) {
        return fusionEnabled()
                ? fusionLauncher()
                : new K8sWrapperBuilder(task)
    }

    protected List<String> classicSubmitCli(TaskRun task) {
        final result = new ArrayList(BashWrapperBuilder.BASH)
        final command = """
            for i in {1..50}; do
                if [ -f ${Escape.path(task.workDir)}/${TaskRun.CMD_RUN} ]; then
                    echo "File found, executing command."
                    exec /bin/bash -ue ${Escape.path(task.workDir)}/${TaskRun.CMD_RUN}
                    exit 0
                else
                    echo "Waiting for file to become available..."
                    sleep 0.5
                fi
            done
            echo "File not found after 50 attempts, failing."
            exit 1
        """.stripIndent()
        result.add("-c".toString().trim())
        result.add(command.toString().trim())
        return result
    }

    protected List<String> getSubmitCommand(TaskRun task) {
        return fusionEnabled()
                ? fusionSubmitCli()
                : classicSubmitCli(task)
    }

    protected static String getSyntheticPodName(TaskRun task) {
        def executionToken = System.getenv("FLYTE_INTERNAL_EXECUTION_ID")
        if (executionToken == null)
            throw new RuntimeException("failed to fetch execution token")
        return "${executionToken}-${task.hash.toString().substring(0, 8)}-${task.index}"
    }

    protected static String getOwner() { OWNER }

    protected Boolean fixOwnership() {
        task.containerConfig.fixOwnership
    }

    /**
     * Creates a Pod specification that executed that specified task
     *
     * @param task A {@link TaskRun} instance representing the task to execute
     * @return A {@link Map} object modeling a Pod specification
     */

    protected Map newSubmitRequest(TaskRun task) {
        def imageName = task.container
        if( !imageName )
            throw new ProcessSubmitException("Missing container image for process `$task.processor.name`")

        try {
            newSubmitRequest0(task, imageName)
        }
        catch( Throwable e ) {
            throw  new ProcessSubmitException("Failed to submit K8s ${resourceType.lower()} -- Cause: ${e.message ?: e}", e)
        }
    }

    protected boolean entrypointOverride() {
        return executor.getK8sConfig().entrypointOverride()
    }

    protected Map newSubmitRequest0(TaskRun task, String imageName) {

        final launcher = getSubmitCommand(task)
        final taskCfg = task.getConfig()

        final clientConfig = client.config
        final builder = new PodSpecBuilder()
            .withImageName(imageName)
            .withPodName(getSyntheticPodName(task))
            .withNamespace(clientConfig.namespace)
            .withServiceAccount(clientConfig.serviceAccount)
            .withLabels(getLabels(task))
            .withAnnotations(getAnnotations())
            .withPodOptions(getPodOptions())
            .withHostMount("/opt/latch-env", "/opt/latch-env")

        builder.withEnv(PodEnv.value("LATCH_NO_CRASH_REPORT", "1"))

        if (System.getenv("LATCH_NF_DEBUG") != "true") {
            def execId = System.getenv("FLYTE_INTERNAL_EXECUTION_ID")
            builder.withEnv(PodEnv.value("FLYTE_INTERNAL_EXECUTION_ID", execId))

            def logDir = System.getenv("LATCH_LOG_DIR")
            builder.withEnv(PodEnv.value("LATCH_LOG_DIR", logDir))
        }

        // when `entrypointOverride` is false the launcher is run via `args` instead of `command`
        // to not override the container entrypoint
        if( !entrypointOverride() ) {
            builder.withArgs(launcher)
        }
        else {
            builder.withCommand(launcher)
        }

        // note: task environment is managed by the task bash wrapper
        // do not add here -- see also #680
        if( fixOwnership() )
            builder.withEnv(PodEnv.value('NXF_OWNER', getOwner()))

        if( SysEnv.containsKey('NXF_DEBUG') )
            builder.withEnv(PodEnv.value('NXF_DEBUG', SysEnv.get('NXF_DEBUG')))

        // add computing resources
        final cpus = taskCfg.getCpus()
        final mem = taskCfg.getMemory()
        final disk = taskCfg.getDisk()
        final acc = taskCfg.getAccelerator()
        if( cpus )
            builder.withCpus(cpus)
        if( mem )
            builder.withMemory(mem)
        if( disk )
            builder.withDisk(disk)
        if( acc )
            builder.withAccelerator(acc)

        final List<String> hostMounts = getContainerMounts()
        for( String mount : hostMounts ) {
            builder.withHostMount(mount,mount)
        }

        if ( taskCfg.time ) {
            final duration = taskCfg.getTime()
            builder.withActiveDeadline(duration.toSeconds() as int)
        }

        if ( fusionEnabled() ) {
            if( fusionConfig().privileged() )
                builder.withPrivileged(true)
            else {
                builder.withResourcesLimits(["nextflow.io/fuse": 1])
            }

            final env = fusionLauncher().fusionEnv()
            for( Map.Entry<String,String> it : env )
                builder.withEnv(PodEnv.value(it.key, it.value))
        }

        return useJobResource()
            ? builder.buildAsJob()
            : builder.build()
    }

    protected PodOptions getPodOptions() {
        // merge the pod options provided in the k8s config
        // with the ones in process config
        def opt1 = k8sConfig.getPodOptions()
        def opt2 = task.getConfig().getPodOptions()
        return opt1 + opt2
    }


    protected Map<String,String> getLabels(TaskRun task) {
        final result = new LinkedHashMap<String,String>(10)
        final labels = k8sConfig.getLabels()
        if( labels ) {
            result.putAll(labels)
        }
        final resLabels = task.config.getResourceLabels()
        if( resLabels )
            result.putAll(resLabels)
        result.'nextflow.io/app' = 'nextflow'
        result.'nextflow.io/runName' = getRunName()
        result.'nextflow.io/taskName' = task.getName()
        result.'nextflow.io/processName' = task.getProcessor().getName()
        result.'nextflow.io/sessionId' = "uuid-${executor.getSession().uniqueId}" as String
        if( task.config.queue )
            result.'nextflow.io/queue' = task.config.queue
        return result
    }

    protected Map getAnnotations() {
        k8sConfig.getAnnotations()
    }

    /**
     * Creates a new K8s pod executing the associated task
     */
    @Override
    @CompileDynamic
    void submit() {
        builder = createBashWrapper(task)
        builder.build()

        final req = newSubmitRequest(task)
        this.dispatcherClient.submitPod(taskExecutionId, req)

        this.status = TaskStatus.SUBMITTED
    }

    @Override
    boolean checkIfRunning() {
        if(isSubmitted()) {
            def s = dispatcherClient.getTaskStatus(taskExecutionId)

            // include terminated states to allow the handler status to progress
            if (['RUNNING', 'SUCCEEDED', 'FAILED'].contains(s)) {
                status = TaskStatus.RUNNING
                return true
            }
        }

        return false
    }

    @Override
    boolean checkIfCompleted() {
        Map s = dispatcherClient.getTaskStatus(taskExecutionId)

        if( ['SUCCEEDED', 'FAILED'].contains(s.status) ) {

            if (s.status == 'FAILED' && s.systemError != null) {
                task.error = new PodUnschedulableException((String) s.systemError, new Exception("failed to launch pod"))
                task.aborted = true
            } else {
                // finalize the task
                task.exitStatus = readExitFile()
                task.stdout = outputFile
                task.stderr = errorFile
            }

            status = TaskStatus.COMPLETED

            return true
        }

        return false
    }

    protected int readExitFile() {
        // If using OFS, files may not be immediately available as mount might be slow
        int attempts = 0
        while (attempts < 50) {
            try {
                def exitText = exitFile.text
                if (exitText.trim()) {
                    log.debug "[K8s] Exit status file has content for task: `$task.name`. Content: $exitText. Exited on attempt $attempts"
                    return exitText as Integer
                }
            }
            catch (Exception e) {
                log.warn "[K8s] Cannot read exitstatus for task: `$task.name`. Retrying with attempt $attempts | ${e.message}"
                sleep(500) // Wait for 0.5 seconds before retrying
                attempts++
            }
        }
        log.warn "[K8s] Failed to read non-empty exitstatus for task: `$task.name` after $attempts attempts"
        return Integer.MAX_VALUE
    }

    /**
     * Terminates the current task execution
     */
    @Override
    void kill() {
        dispatcherClient.updateTaskStatus(taskExecutionId, 'ABORTING')
    }
}
