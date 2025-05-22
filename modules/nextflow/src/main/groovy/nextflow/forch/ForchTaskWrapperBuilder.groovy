package nextflow.forch

import java.nio.file.Path

import nextflow.executor.BashWrapperBuilder
import nextflow.executor.SimpleFileCopyStrategy
import nextflow.processor.TaskBean
import nextflow.processor.TaskRun

class ForchTaskWrapperBuilder extends BashWrapperBuilder {
    // entirely lifted from AWS Batch Wrapper
    ForchTaskWrapperBuilder(TaskBean bean) {
        super(bean, new SimpleFileCopyStrategy())
        // enable the copying of output file to the S3 work dir
        if( scratch==null )
            scratch = true

        // include task script as an input to force its staging in the container work directory
        bean.inputFiles[TaskRun.CMD_SCRIPT] = bean.workDir.resolve(TaskRun.CMD_SCRIPT)
        // add the wrapper file when stats are enabled
        // NOTE: this must match the logic that uses the run script in BashWrapperBuilder
        if( isTraceRequired() ) {
            bean.inputFiles[TaskRun.CMD_RUN] = bean.workDir.resolve(TaskRun.CMD_RUN)
        }
        // include task stdin file
        if( bean.input != null ) {
            bean.inputFiles[TaskRun.CMD_INFILE] = bean.workDir.resolve(TaskRun.CMD_INFILE)
        }
    }
}
