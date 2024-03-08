package nextflow.dagGeneration

import java.util.concurrent.atomic.AtomicLong

import groovyjarjarantlr4.v4.misc.OrderedHashMap
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
        Input, // rename
        Output,
        Merge,
        Tuple,
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

// TupleVertices will NEVER be added to the graph
// they are strictly for internal plumbing
class TupleVertex extends Vertex {
    Map<String, Vertex> members = new OrderedHashMap<String, Vertex>()

    TupleVertex(String label, Map<String, Vertex> members) {
        super(Type.Tuple, label, null, null)

        this.members = members
    }

    void unpack(String prefix, Map<String, Vertex> res) {
        for (def entry: this.members) {
            Vertex v = entry.value
            String label = "${prefix}${entry.key}"

            if (v instanceof TupleVertex) {
                v.unpack("$label.", res)
            } else {
                res[label] = v
            }
        }
    }

    Map<String, Vertex> unpack() {
        Map<String, Vertex> res = new OrderedHashMap<String, Vertex>()
        this.unpack("", res)
        return res
    }

    // since this uses unpack, this flattens all the layers, which is technically wrong bc Nextflow is weird only flattens
    // one layer deep when doing tuple unpacking (ie [[[1], 2], [3, 4]] -> [[1], 2, 3, 4]), however
    // i think this is esoteric enough that we shouldn't support it unless someone really wants it
    TupleVertex remap(Map<Vertex, Vertex> mapping) {
        def xs = this.unpack()
        return new TupleVertex(this.label, xs.collectEntries { _, x -> mapping[x]})
    }
}
