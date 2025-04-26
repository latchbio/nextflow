package nextflow.forch

import java.nio.file.Path

import nextflow.executor.BashFunLib
import nextflow.executor.SimpleFileCopyStrategy
import nextflow.util.Escape

class ForchFileCopyStrategy extends SimpleFileCopyStrategy {

    @Override
    String getBeforeStartScript() {
        def lib = new BashFunLib().coreLib()

        return lib + "\n\n" + """\
            nxf_s5cmd_upload() {
                local name=\$1
                local s3path=\$2
                if [[ "\$name" == - ]]; then
                    echo 's5cmd --no-verify-ssl pipe "\$s3path"'
                    s5cmd --no-verify-ssl pipe "\$s3path"
                elif [[ -d "\$name" ]]; then
                    s5cmd --no-verify-ssl cp "\$name" "\$s3path/"
                else                    
                    s5cmd --no-verify-ssl cp "\$name" "\$s3path/\$name"
                fi
            }
    
            nxf_s5cmd_download() {
                local source=\$1
                local target=\$2
                local file_name=\$(basename \$1)
                local is_dir=\$(s5cmd --no-verify-ssl ls \$source | grep -F "DIR  \${file_name}/" -c)
                if [[ \$is_dir == 1 ]]; then
                    s5cmd --no-verify-ssl cp "\$source/*" "\$target"
                else
                    s5cmd --no-verify-ssl cp "\$source" "\$target"
                fi
            }
        
        
        """.stripIndent()
    }

    @Override
    String getStageInputFilesScript(Map<String, Path> inputFiles) {
        def result = 'downloads=(true)\n'
        result += super.getStageInputFilesScript(inputFiles) + '\n'
        result += 'nxf_parallel "${downloads[@]}"\n'
        return result
    }

    @Override
    protected String stageInCommand(String source, String target, String mode) {
        return "downloads+=(\"nxf_s5cmd_download s3:/${Escape.path(source)} ${Escape.path(target)}\")"
    }

    @Override
    String getUnstageOutputFilesScript(List<String> outputFiles, Path targetDir) {
        final patterns = normalizeGlobStarPaths(outputFiles)

        if( !patterns )
            return null

        final escape = new ArrayList(outputFiles.size())
        for( String it : patterns )
            escape.add( Escape.path(it) )

        return """\
            uploads=()
            IFS=\$'\\n'
            for name in \$(eval "ls -1d ${escape.join(' ')}" | sort | uniq); do
                uploads+=("nxf_s5cmd_upload '\$name' s3:/${Escape.path(targetDir)}")
            done
            unset IFS
            nxf_parallel "\${uploads[@]}"
        """.stripIndent(true)
    }

    @Override
    String touchFile(Path file) {
        return "echo start | s5cmd --no-verify-ssl pipe s3:/${Escape.path(file)}"
    }

    @Override
    String fileStr( Path path ) {
        Escape.path(path.getFileName())
    }

    @Override
    String copyFile( String name, Path target ) {
        "s5cmd --no-verify-ssl cp ${Escape.path(name)} s3:/${Escape.path(target)}"
    }

    @Override
    String exitFile(Path file) {
        return "| s5cmd --no-verify-ssl pipe s3:/${Escape.path(file)} || true"
    }

    @Override
    String pipeInputFile(Path file) {
        return " < ${Escape.path(file.getFileName())}"
    }
}
