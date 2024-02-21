package nextflow.dagGeneration

import org.codehaus.groovy.ast.expr.MethodCallExpression

class NFEntity {
    static enum Type {
        Process,
        Workflow
    }

    public Type type
    public MethodCallExpression definition
    public String unaliased
    public String module

    public List<String> outputs

    NFEntity( Type type, MethodCallExpression definition, List<String> outputs, String module, String unaliased ) {
        this.type = type
        this.definition = definition
        this.outputs = outputs
        this.module = module
        this.unaliased = unaliased
    }

    static NFEntity copy(NFEntity entity) {
        return new NFEntity(
            entity.type,
            entity.definition,
            entity.outputs,
            entity.module,
            entity.unaliased,
        )
    }
}


class Scope {
    Scope parent;
    Map<String, Vertex> bindings;

    Scope(Scope parent) {
        this.parent = parent
        this.bindings = new HashMap<String, Vertex>()
    }

    Scope() {
        this.parent = null
        this.bindings = new HashMap<String, Vertex>()
    }

    Vertex get(String name) {
        def ret = bindings.get(name)
        if (ret != null) return ret

        return parent?.get(name)
    }

    void set(String name, Vertex v) {
        this.bindings[name] = v
    }
}
