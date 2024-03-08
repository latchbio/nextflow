package nextflow.dagGeneration

import groovyjarjarantlr4.v4.misc.OrderedHashMap

class ScopeVariable {
    Vertex vertex

    ScopeVariable(Vertex vertex) {
        this.vertex = vertex
    }

    ScopeVariable remap(Map<Vertex, Vertex> mapping) {
        return new ScopeVariable(mapping[this.vertex])
    }

    @Override
    boolean equals(Object obj) {
        return (
            (obj instanceof ScopeVariable) &&
            (obj.vertex != null) &&
            (this.vertex.id == obj.vertex.id)
        )
    }
}

class PropertyVariable extends ScopeVariable {
    Integer index

    PropertyVariable(Vertex vertex, Integer index) {
        super(vertex)

        this.index = index
    }

    PropertyVariable remap(Map<Vertex, Vertex> mapping) {
        return new PropertyVariable(mapping[this.vertex], this.index)
    }

    @Override
    boolean equals(Object obj) {
        return (
            (obj instanceof PropertyVariable) &&
            (obj.vertex != null) &&
            (this.vertex.id == obj.vertex.id) &&
            (this.index == obj.index)
        )
    }
}

class ProcessVariable extends ScopeVariable {
    OrderedHashMap<String, PropertyVariable> properties

    ProcessVariable(Vertex vertex, OrderedHashMap<String, PropertyVariable> properties) {
        super(vertex)

        this.properties = properties
    }

    ProcessVariable remap(Map<Vertex, Vertex> mapping) {
        def newProps = new OrderedHashMap<String, PropertyVariable>();
        for (def prop: this.properties) {
            newProps[prop.key] = prop.value.remap(mapping)
        }

        return new ProcessVariable(mapping[this.vertex], newProps)
    }

    @Override
    boolean equals(Object obj) {
        return (
            (obj instanceof ProcessVariable) &&
            (obj.vertex != null) &&
            (this.vertex.id == obj.vertex.id) &&
            this.properties == obj.properties
        )
    }
}

// handling for branch/multimap outputs
class SpecialOperatorVariable extends ProcessVariable {
    SpecialOperatorVariable(Vertex vertex, OrderedHashMap<String, PropertyVariable> properties) {
        super(vertex, properties)
    }
}

class SubWorkflowVariable extends ScopeVariable {
    OrderedHashMap<String, ScopeVariable> properties

    SubWorkflowVariable(OrderedHashMap<String, ScopeVariable> properties) {
        super(null)

        this.properties = properties
    }

    SubWorkflowVariable remap(Map<Vertex, Vertex> mapping) {
        def newProps = new OrderedHashMap<String, ScopeVariable>();
        for (def prop: this.properties) {
            newProps[prop.key] = prop.value.remap(mapping)
        }

        return new SubWorkflowVariable(newProps)
    }

    @Override
    boolean equals(Object obj) {
        return (
            (obj instanceof SubWorkflowVariable) &&
            this.properties == obj.properties
        )
    }
}
