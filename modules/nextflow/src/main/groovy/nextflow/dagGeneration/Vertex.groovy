package nextflow.dagGeneration

import java.util.concurrent.atomic.AtomicLong

import org.codehaus.groovy.ast.stmt.Statement

class Vertex {
    static private AtomicLong nextID = new AtomicLong()

    static enum Type {
        Process,
        Operator,
        SubWorkflow,
        Generator, // Channel.from, Channel.of, etc.
        Literal,

        // Internal Plumbing
        Conditional,
        Input,
        Output,
        Merge,
    }

    final long id = nextID.getAndIncrement()

    public Type type
    public String label
    public Statement call
    public List<Statement> ret
    public List<String> outputNames = []
    public String module = ""
    public String unaliased = ""

    Vertex( Type type, String label, Statement call, List<Statement> ret) {
        this.label = label
        this.type = type
        this.call = call
        this.ret = ret
    }

    Vertex( Type type, String label, Statement call, List<Statement> ret, List<String> outputNames, String module, String unaliased) {
        this.type = type
        this.label = label
        this.call = call
        this.ret = ret
        this.outputNames = outputNames
        this.module = module
        this.unaliased = unaliased
    }

    static Vertex clone(Vertex other) {
        return new Vertex(
            other.type,
            other.label,
            other.call,
            other.ret,
            other.outputNames,
            other.module,
            other.unaliased
        )
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

class OutputVertex extends Vertex {
    OutputVertex(String label, List<String> outputNames ) {
        super(Type.Output, label, null, null)

        this.outputNames = outputNames
    }
}

class MergeVertex extends Vertex {
    MergeVertex(String label) {
        super(Type.Merge, label, null, null)
    }
}