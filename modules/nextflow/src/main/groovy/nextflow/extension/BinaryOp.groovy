package nextflow.extension

import groovy.transform.CompileStatic
import groovyx.gpars.dataflow.DataflowReadChannel
import groovyx.gpars.dataflow.DataflowWriteChannel
import groovyx.gpars.dataflow.operator.DataflowProcessor
import static nextflow.extension.DataflowHelper.createOpParams
import static nextflow.extension.DataflowHelper.newOperator
import static nextflow.extension.DataflowHelper.stopErrorListener

@CompileStatic
class BinaryOpClosure extends Closure {
    private String overrideOpName
    private Closure operation

    // todo(ayush): this list is not exhaustive
    private Map<String, String> overridableOps = [
        "+": "plus",
        "-": "minus",
        "*": "multiply",
        "/": "div",
        "%": "mod",
        "**": "power",
        "|": "or",
        "&": "and",
        "^": "xor",
        // todo(ayush): boolean && and || don't seem to have intrinsic method names
    ]

    private Map<String, Closure> operations = [
        "||": {l, r -> l || r},
        "&&": {l, r -> l && r},
    ] as Map<String, Closure>

    BinaryOpClosure(String op) {
        super(null, null);
        this.overrideOpName = overridableOps.get(op)
        this.operation = operations.get(op)

        if (this.overrideOpName == null && this.operation == null) {
            throw new UnsupportedOperationException("unknown binary operator '$op'")
        }
    }

    @Override
    int getMaximumNumberOfParameters() {
        return 2
    }

    @Override
    Object call(final Object... args) {
        if (args.size() != 2)
            throw new UnsupportedOperationException("binary operation requires exactly two arguments")

        def result
        if (this.overrideOpName != null) {
            result = args[0].invokeMethod(this.overrideOpName, args[1])
        } else {
            result = this.operation.call(args[0], args[1])
        }

        ((DataflowProcessor) getDelegate()).bindAllOutputsAtomically(result);
        return result;
    }

}

@CompileStatic
class BinaryOp {
    private DataflowReadChannel left
    private DataflowReadChannel right
    private String op

    BinaryOp(DataflowReadChannel left, DataflowReadChannel right, String op) {
        this.left = left
        this.right = right
        this.op = op
    }

    BinaryOp(DataflowReadChannel left, DataflowReadChannel right, String op, Boolean swap) {
        if (swap) {
            this.left = right
            this.right = left
        } else {
            this.left = left
            this.right = right
        }

        this.op = op
    }

    DataflowWriteChannel apply() {
        final result = CH.createBy(left)
        final List<DataflowReadChannel> inputs = [left, right]
        final action =  new BinaryOpClosure(op)

        final listener = stopErrorListener(left, result)
        final params = createOpParams(inputs, result, listener)
        newOperator(params, action)

        return result
    }

}
