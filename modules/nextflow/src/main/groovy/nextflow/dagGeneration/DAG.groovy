package nextflow.dagGeneration

import groovyjarjarantlr4.v4.misc.OrderedHashMap

interface EdgeLike<T> {
    T getFrom()
    T getTo()
    void setFrom(T from)
    void setTo(T to)
}

class DAG<V, E extends EdgeLike<V>> {
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
