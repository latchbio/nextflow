package nextflow.extension

import groovyx.gpars.dataflow.DataflowReadChannel
import groovyx.gpars.dataflow.DataflowWriteChannel
import groovyx.gpars.dataflow.operator.DataflowProcessor
import static nextflow.extension.DataflowHelper.createOpParams
import static nextflow.extension.DataflowHelper.newOperator
import static nextflow.extension.DataflowHelper.stopErrorListener

class BooleanOp {

    private DataflowReadChannel input

    BooleanOp (DataflowReadChannel input) {
        this.input = input
    }

    DataflowWriteChannel apply() {
        final result = CH.createBy(input)
        final action = {Object it ->
            Boolean res = !!it // superhack
            ((DataflowProcessor) getDelegate()).bindAllOutputsAtomically(res)
            return res
        }

        final listener = stopErrorListener(input, result)
        final params = createOpParams([input], result, listener)
        newOperator(params, action)

        return result
    }
}
