package nextflow.k8s

import nextflow.executor.BashFunLib
import nextflow.executor.SimpleFileCopyStrategy

import java.nio.file.Path
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.processor.TaskBean
import nextflow.util.Escape

/**
 * Defines the script operation to handle file when running in the Cirrus cluster
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
class K8sFileCopyStrategy extends SimpleFileCopyStrategy {

    private Path remoteBinDir

    K8sFileCopyStrategy(TaskBean task, Path remoteBinDir) {
        super(task)
        this.remoteBinDir = remoteBinDir
    }

    String getBeforeStartScript() {
        String script = new BashFunLib().coreLib()  + '\n'
        script += super.getBeforeStartScript()  + '\n'
        return script
    }

    /**
     * {@inheritDoc}
     */
    @Override
    String getEnvScript(Map environment, boolean container) {
        if( container )
            throw new IllegalArgumentException("Parameter `container` not supported by ${this.class.simpleName}")

        final result = new StringBuilder()
        final copy = environment ? new LinkedHashMap<String,String>(environment) : Collections.<String,String>emptyMap()
        final path = copy.containsKey('PATH')
        // remove any external PATH
        if( path )
            copy.remove('PATH')

        if( remoteBinDir ) {
            result << "nxf_cp_retry nxf_latch_download latch://${remoteBinDir} \$PWD/nextflow-bin\n"
            result << "chmod +x \$PWD/nextflow-bin/* || true\n"
            result << "export PATH=\$PWD/nextflow-bin:\$PATH\n"
        }


        // finally render the environment
        final envSnippet = super.getEnvScript(copy,false)
        if( envSnippet )
            result << envSnippet
        return result.toString()
    }

    @Override
    String getStageInputFilesScript(Map<String, Path> inputFiles) {
        def result = 'downloads=(true)\n'
        result += super.getStageInputFilesScript(inputFiles) + '\n'
        result += 'nxf_parallel "${downloads[@]}"\n'
        return result
    }

    /**
     * {@inheritDoc}
     */
    @Override
    String stageInputFile( Path path, String targetName ) {
        // third param should not be escaped, because it's used in the grep match rule
        def stage_cmd = "downloads+=(\"nxf_cp_retry nxf_latch_download latch://${Escape.path(path)} ${Escape.path(targetName)}\")"
        return stage_cmd
    }

    /**
     * {@inheritDoc}
     */
    @Override
    String getUnstageOutputFilesScript(List<String> outputFiles, Path targetDir) {

        final patterns = normalizeGlobStarPaths(outputFiles)
        // create a bash script that will copy the out file to the working directory
        log.trace "[Latch] Unstaging file path: $patterns"

        if( !patterns )
            return null

        final escape = new ArrayList(outputFiles.size())
        for( String it : patterns )
            escape.add( Escape.path(it) )

        return """\
            uploads=()
            IFS=\$'\\n'
            for name in \$(eval "ls -1d ${escape.join(' ')}" | sort | uniq); do
                uploads+=("nxf_latch_upload '\$name' latch://${Escape.path(targetDir)}")
            done
            unset IFS
            nxf_parallel "\${uploads[@]}"
            """.stripIndent(true)
    }

    /**
     * {@inheritDoc}
     */
    @Override
    String touchFile( Path file ) {
        "echo start | nxf_latch_upload - latch://${Escape.path(file)}"
    }

    /**
     * {@inheritDoc}
     */
    @Override
    String fileStr( Path path ) {
        Escape.path(path.getFileName())
    }

    /**
     * {@inheritDoc}
     */
    @Override
    String copyFile( String name, Path target ) {
        "nxf_latch_upload ${Escape.path(name)} latch://${Escape.path(target.getParent())}"
    }

    static String uploadCmd( String source, Path target ) {
        "nxf_latch_upload ${Escape.path(source)} latch://${Escape.path(target)}"
    }

    /**
     * {@inheritDoc}
     */
    String exitFile( Path path ) {
        "| nxf_latch_upload - latch://${Escape.path(path)} || true"
    }

    /**
     * {@inheritDoc}
     */
    @Override
    String pipeInputFile( Path path ) {
        " < ${Escape.path(path.getFileName())}"
    }
}
