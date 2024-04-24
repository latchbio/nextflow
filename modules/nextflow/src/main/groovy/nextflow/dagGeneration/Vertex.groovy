package nextflow.dagGeneration

import java.util.concurrent.atomic.AtomicLong

import org.codehaus.groovy.ast.stmt.Statement

class Vertex implements VertexLike<Vertex> {
    static private AtomicLong nextID = new AtomicLong()

    static enum Type {
        Process,
        Operator,
        SubWorkflow,
        Generator, // Channel.from, Channel.of, literal expressions, etc.
        Conditional, // if statements, ternary expressions

        // Internal Plumbing
        Input, // rename
        Merge,
    }

    final long id = nextID.getAndIncrement()

    public Type type
    public String label
    public Statement call
    public List<Statement> ret
    public List<String> outputNames = []
    public String subWorkflowName = null
    public String subWorkflowPath = null
    public String module = ""
    public String unaliased = ""
    public Integer cpus = null
    public Long memoryBytes = null

    Vertex( Type type, String label, Statement call, List<Statement> ret) {
        if (label == "") {
            label = "\"\""
        }

        this.label = label
        this.type = type
        this.call = call
        this.ret = ret
    }

    Vertex( Type type, String label, Statement call, List<Statement> ret, List<String> outputNames, String subWorkflowName, String subWorkflowPath, String module, String unaliased, Integer cpus, Long memoryBytes) {
        this.type = type
        this.label = label
        this.call = call
        this.ret = ret
        this.outputNames = outputNames
        this.subWorkflowName = subWorkflowName
        this.subWorkflowPath = subWorkflowPath
        this.module = module
        this.unaliased = unaliased
        this.cpus = cpus
        this.memoryBytes = memoryBytes
    }

    Vertex make_clone() {
        return new Vertex(
            type,
            label,
            call,
            ret,
            outputNames,
            subWorkflowName,
            subWorkflowPath,
            module,
            unaliased,
            cpus,
            memoryBytes
        )
    }

    String toString() {
        return "${this.type}Vertex(id=${this.id})"
    }
}

class ConditionalVertex extends Vertex {
    ConditionalVertex( String label, Statement stmt, List<Statement> ret ) {
        super(Type.Conditional, label, stmt, ret)
    }
}

class ProcessVertex extends Vertex {

    ProcessVertex ( String label, Statement stmt, List<Statement> ret, List<String> outputNames, String module, String unaliased ) {
        super(Type.Process, label, stmt, ret)

        this.outputNames = outputNames
        this.module = module
        this.unaliased = unaliased
    }
}

class MergeVertex extends Vertex {
    MergeVertex(String label) {
        super(Type.Merge, label, null, null)
    }
}
