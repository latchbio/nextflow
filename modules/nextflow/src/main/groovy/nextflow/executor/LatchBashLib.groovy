package nextflow.executor

import groovy.transform.CompileStatic

/**
 * Latch Data helper class
 */
@CompileStatic
class LatchBashLib extends BashFunLib<LatchBashLib> {
    //final private String cli = "/opt/latch-env/bin/latch"
    final private String cli = "latch"

    /**
     * Implement LData upload/download helper using `latch cp` CLI tool
     *
     * @return The Bash script implementing the S3 helper functions
     */
    protected String latchLib() {
        """
        # latch helper
        nxf_latch_upload() {
            local name=\$1
            local latch_path=\$2
            local temp_file=\$(mktemp)

            if [[ "\$name" == - ]]; then
              cat > "\$temp_file"
              $cli cp --progress total "\$temp_file" "\$latch_path"
            else
              local target_path="\$latch_path/\$name"
              $cli mkdirp "\$(dirname "\$target_path")"
              $cli cp --progress total "\$name" "\$target_path"
            fi
            
            rm -f "\$temp_file"
        }
        
        nxf_latch_download() {
            local source=\$1
            local target=\$2
            $cli cp --progress total "\$source" "\$target"
        }
        """.stripIndent(true)
    }

    @Override
    String render() {
        return super.render() + latchLib()
    }

    static String script() {
        new LatchBashLib().render()
    }
}
