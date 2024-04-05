package nextflow.dagGeneration

class Edge implements EdgeLike<Vertex, Edge> {
    String label
    Vertex from
    Vertex to

    Edge( String label, Vertex from, Vertex to ) {
        this.label = label
        this.from = from
        this.to = to
    }

    Edge make_clone() {
        return new Edge(label, from, to)
    }
}

class ConditionalEdge extends Edge {

    /*
     *  Which branch of the if-else block the destination vertex is part of.
     *  Succinctly: run the vertex at this.to iff cond == branch
     */
    boolean branch

    ConditionalEdge( String label, Vertex cond, Vertex to, boolean branch ) {
        super("Conditional: $label - Branch: $branch", cond, to)

        this.branch = branch
    }

    @Override
    ConditionalEdge make_clone() {
        return new ConditionalEdge(label, from, to, branch)
    }
}
