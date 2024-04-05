package nextflow.dagGeneration

import groovyjarjarantlr4.v4.misc.OrderedHashMap

interface VertexLike<THIS extends VertexLike<THIS>> {
    long getId()
    THIS make_clone()
}

interface EdgeLike<T, THIS extends EdgeLike<T, THIS>> {
    T getFrom()
    T getTo()
    void setFrom(T from)
    void setTo(T to)
    THIS make_clone()
}

class DAG<V extends VertexLike<V>, E extends EdgeLike<V, E>> {
    List<V> vertices
    List<E> edges

    private Map<V, List<E>> downstream
    private Map<V, List<E>> upstream

    private Set<V> vertexSet

    DAG() {
        vertices = []
        edges = []

        downstream = new HashMap<V, List<E>>().withDefault {new ArrayList<E>()}
        upstream = new HashMap<V, List<E>>().withDefault {new ArrayList<E>()}
        vertexSet = new HashSet<V>();
    }


    DAG<V,E> make_clone() {
        DAG<V,E> newDAG = new DAG()
        Map<V,V> vertexMapping = new OrderedHashMap<V,V>()

        for (V v: this.vertices) {
            V newV = v.make_clone()
            vertexMapping[v] = newV
            newDAG.addVertex(newV)
        }

        for (E e: this.edges) {
            E newE = e.make_clone()
            newE.from = vertexMapping[newE.from]
            newE.to = vertexMapping[newE.to]
            newDAG.addEdge(newE)
        }

        return newDAG
    }

    void addVertex(V v) {
        vertices << v
        vertexSet << v
    }

    void addEdge(E e) {
        if (!(e.from in vertexSet) || !(e.to in vertexSet)) {
            throw new Exception("Invalid edge: ${e.from} or ${e.to} are not known vertices")
        }

        this.edges << e

        downstream[e.from] << e
        upstream[e.to] << e
    }

    List<E> getDownstreamEdges(V v) {
        downstream[v]
    }

    List<E> getUpstreamEdges(V v) {
        upstream[v]
    }
}
