package nextflow.dagGeneration

import org.codehaus.groovy.ast.expr.MethodCallExpression

class NFEntity {
    static enum Type {
        Process,
        Workflow
    }

    public boolean called = false

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
}


class Scope {
    Scope parent;
    Map<String, ScopeVariable> bindings;

    Scope(Scope parent) {
        this.parent = parent
        this.bindings = new HashMap<String, ScopeVariable>()
    }

    Scope() {
        this.parent = null
        this.bindings = new HashMap<String, ScopeVariable>()
    }

    ScopeVariable get(String name) {
        def ret = bindings.get(name)
        if (ret != null) return ret

        return parent?.get(name)
    }

    void set(String name, ScopeVariable v) {
        this.bindings[name] = v
    }

    void forEach(Closure cb) {
        if (this.parent != null) {
            this.parent.forEach(cb)
        }
        this.bindings.forEach(cb)
    }
}
