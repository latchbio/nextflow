package nextflow.dagGeneration

import java.util.concurrent.atomic.AtomicLong

import groovy.json.JsonBuilder
import groovy.util.logging.Slf4j
import org.codehaus.groovy.ast.expr.ArgumentListExpression
import org.codehaus.groovy.ast.expr.BinaryExpression
import org.codehaus.groovy.ast.expr.BooleanExpression
import org.codehaus.groovy.ast.expr.ClosureExpression
import org.codehaus.groovy.ast.expr.ConstantExpression
import org.codehaus.groovy.ast.expr.Expression
import org.codehaus.groovy.ast.expr.MethodCallExpression
import org.codehaus.groovy.ast.expr.TupleExpression
import org.codehaus.groovy.ast.expr.VariableExpression
import org.codehaus.groovy.ast.stmt.BlockStatement
import org.codehaus.groovy.ast.stmt.ExpressionStatement
import org.codehaus.groovy.ast.stmt.IfStatement
import org.codehaus.groovy.ast.stmt.Statement
import org.codehaus.groovy.syntax.Types

@Slf4j
class DAGGenerator {

    class DAGGenerationException extends Exception {
        DAGGenerationException(String msg) {
            super(msg)
        }
    }

    class Vertex {
        static private AtomicLong nextID = new AtomicLong()

        static enum Type {
            Process,
            Operator,
            Conditional,
            Generator, // Channel.from, Channel.of, etc.
            Input,
        }

        final long id = nextID.getAndIncrement()

        public Type type
        public String label

        Vertex( Type type, String label ) {
            this.label = label
            this.type = type
        }
    }

    class ConditionalVertex extends Vertex {
        BooleanExpression condition

        ConditionalVertex( String label, BooleanExpression condition ) {
            super(Type.Conditional, "Conditional: $label")

            this.condition = condition
        }
    }

    class Edge {
        String label
        Vertex from
        Vertex to

        Edge( String label, Vertex from, Vertex to ) {
            this.label = label
            this.from = from
            this.to = to
        }
    }

    class ConditionalEdge extends Edge {

        /*
         *  Which branch of the if-else block the destination vertex is part of.
         *  Succinctly: run the vertex at this.to iff cond == branch
         */
        boolean branch

        ConditionalEdge( String label, ConditionalVertex cond, Vertex to, boolean branch ) {
            super("Conditional: $label - Branch: $branch", cond, to)

            this.branch = branch
        }
    }

    String workflowName
    Set<String> processNames

    final Set<String> channelFactories = new HashSet([
        "empty",
        "from",
        "fromList",
        "fromPath",
        "fromFilePairs",
        "fromSRA",
        "of",
        "value",
        "watchPath"
    ])
    final Set<String> operatorNames = new HashSet([
        "filter",
        "randomSample",
        "take",
        "unique",
        "collect",
        "groupTuple",
        "reduce",
        "splitCsv",
        "splitJson",
        "splitText",
        "combine",
        "concat",
        "join",
        "mix",
        "branch",
        "multiMap",
        "count",
        "max",
        "min",
        "sum",
        "ifEmpty",
        "map",
        "view"
        // "set" is excluded on purpose as it does not warrant adding a vertex to the graph
    ])

    // todo(ayush): handle
    final Set<String> specialOperatorNames = new HashSet(["set", "branch", "multiMap"])

    List<Edge> edges = new ArrayList<>()
    List<Vertex> vertices = new ArrayList<>()
    List<Vertex> inputNodes = new ArrayList<>()

    /*
     *  Maps channel/variable names -> vertex that most recently output it
     */
    Map<String, Vertex> producers = new LinkedHashMap<>().withDefault { null }

    /*
     * cond in activeConditionals => cond is active
     * activeConditionals[cond] = which branch we are currently processing
     */
    Map<ConditionalVertex, Boolean> activeConditionals = new LinkedHashMap<>()

    /*
     * State management
     */
    private Expression parent

    DAGGenerator(String workflowName, Set<String> processNames) {
        this.workflowName = workflowName
        this.processNames = processNames
    }

    Vertex visitBinaryExpression (BinaryExpression expr) {
        // currently supports:
        // - a = expr
        // - (a, b, ..., z) = expr

        switch (expr.operation.type) {
            case Types.EQUAL:
                // assignment should update producers

                def res = visitExpression(expr.rightExpression)

                if (expr.leftExpression instanceof VariableExpression) {
                    def v = expr.leftExpression as VariableExpression

                    producers[v.name] = res;
                } else if (expr.leftExpression instanceof TupleExpression) {
                    def t = expr.leftExpression as TupleExpression

                    for (def sub: t.expressions) {
                        def v = sub as VariableExpression
                        producers[v.name] = res;
                    }
                } else {
                    throw new DAGGenerationException("Cannot handle left expression in binary expression of type $expr.leftExpression.class: $expr.leftExpression.text ")
                }
                break
            case Types.PIPE:
                // todo(ayush): handle
            default:
                // todo(ayush): warn but continue instead?
                throw new DAGGenerationException("Cannot handle operator in binary expression of type ${expr.operation.text}")

        }
    }

    Vertex visitMethodCallExpression (MethodCallExpression expr) {
        // a method call expression should add either an operator node or a process node,
        // exceptions:
        // - Anonymous channel factories like Channel.of etc.

        def objProducer = visitExpression(expr.objectExpression)

        // todo(ayush): is this even necessary?
        //   - are there any cases where we need to evaluate an expression to figure out which method to call?
        // visitExpression(expr.method)

        Vertex v = null;
        if (expr.methodAsString in this.processNames) {
            v = new Vertex(Vertex.Type.Process, expr.methodAsString)
        } else if (expr.methodAsString in this.operatorNames) {
            v = new Vertex(Vertex.Type.Operator, expr.methodAsString)
        } else if (expr.objectExpression.text == "Channel" && expr.methodAsString in this.channelFactories) {
            v = new Vertex(Vertex.Type.Generator, expr.text)
        }

        if (expr.methodAsString == "set") {
            if (objProducer == null)
                return null;

            def lst = (ArgumentListExpression) expr.arguments;

            if (lst.size() != 1 || !(lst[0] instanceof ClosureExpression))
                throw new DAGGenerationException("Cannot parse non-closure argument(s) to Channel.set(): $lst")

            ClosureExpression ce = lst[0] as ClosureExpression
            BlockStatement b = ce.code as BlockStatement

            if (b.statements.size() != 1)
                throw new DAGGenerationException("Cannot parse closure argument to Channel.set() w/ more than one statement: $b")

            ExpressionStatement stmt = b.statements[0] as ExpressionStatement

            if (!(stmt.expression instanceof VariableExpression))
                throw new DAGGenerationException("Statement in closure argument to Channel.set() must be a variable: $b")

            VariableExpression ve = stmt.expression as VariableExpression
            producers[ve.name] = objProducer
        }

        if (v != null) {
            addVertex(v)
            if (objProducer != null)
                edges.add(new Edge(expr.objectExpression.text, objProducer, v))
        }

        // a TupleExpression should really be named RecordExpression or MapExpression but w/e
        if (expr.arguments instanceof TupleExpression) {
            // handles operators which take in records, e.g. collectFile()
            return v
        }

        def lst = (ArgumentListExpression) expr.arguments;
        for (def x: lst) {
            def producer = visitExpression(x)
            if (v == null || producer == null) continue

            edges.add(new Edge(x.text, producer, v))
        }

        return v
    }

    Vertex visitVariableExpression(VariableExpression expr) {
        // visiting a variable expression var should look up
        // and return the most recent producer of var

        return producers[expr.name]
    }

    Vertex visitClosureExpression(ClosureExpression expr) {

        // for the most part we don't need to care about closures, except for when the parent expr is a MCE and the method is
        // - branch: we need to figure out what the forks are called
        // - multiMap: same thing
        // - set: update the producer of the returned variable name to be the producer of the left expression
        // ^ these are handled in the MCE logic directly

        // todo(ayush): what are the cases where a closure has a nontrivial producer?
        //  - can you reference other channels within closures?
        return null
    }


    Vertex visitExpression(Expression expr) {
        def old = parent;
        parent = expr

        println("${expr.class}: $expr.text")

        Vertex res = null;
        switch (expr) {
            case BinaryExpression:
                res = visitBinaryExpression(expr)
                break
            case MethodCallExpression:
                res = visitMethodCallExpression(expr)
                break
            case VariableExpression:
                res = visitVariableExpression(expr)
                break
            case ClosureExpression:
                res = visitClosureExpression(expr)
                break

            case ConstantExpression:
            default:
                break
        }

        parent = old
        return res
    }

    void addVertex(Vertex v) {
        this.vertices.add(v)

        this.activeConditionals.forEach {cond, branch ->
            this.edges.add(new ConditionalEdge(cond.condition.text, cond, v, branch))
        }
    }

    void visitStatement(Statement stmt) {
        switch (stmt) {
            case ExpressionStatement:
                visitExpression(stmt.expression)
                break
            case IfStatement:
                def cond = new ConditionalVertex(stmt.booleanExpression.text, stmt.booleanExpression)
                addVertex(cond)

                def ifBlock = stmt.ifBlock as BlockStatement
                this.activeConditionals[cond] = true
                for (def x: ifBlock.statements)
                    visitStatement(x)

                // guard against ifs without else blocks
                if (stmt.elseBlock instanceof BlockStatement) {
                    def elseBlock = stmt.elseBlock as BlockStatement
                    this.activeConditionals[cond] = false
                    for (def x: elseBlock.statements)
                        visitStatement(x)
                }

                this.activeConditionals.remove(cond)
        }
    }

    void visit(Statement stmt) {
        try {
            visitStatement(stmt)
        } catch (DAGGenerationException e) {
            log.error """\
            There was an error parsing your workflow body - please ensure you don't have any typos. 
            If the offending line below seems correct, please reach out to us at support@latch.bio.
            
            Offending Line:
            L$stmt.lineNumber: $stmt.text

            Error:
            $e
            """.stripIndent()

            System.exit(1)
        }
    }


    void addInputNode(Statement stmt) {
        if (stmt instanceof ExpressionStatement) {
            def expr = stmt.expression
            if (expr instanceof VariableExpression) {
                def v = new Vertex(Vertex.Type.Input, expr.name)
                inputNodes.add(v)
                addVertex(v)
                producers[expr.name] = v
            }
        }
    }


    void writeDAG() {
        def vbs = new ArrayList<JsonBuilder>()
        def ebs = new ArrayList<JsonBuilder>()

        for (Vertex v: this.vertices) {
            def vb = new JsonBuilder()

            vb {
                id v.id
                label v.label
                type v.type
            }

            vbs.add(vb)
        }

        for (Edge e: this.edges) {
            def eb = new JsonBuilder()

            eb {
                label e.label
                from e.from.id
                to e.to.id
            }

            ebs.add(eb)
        }

        def gb = new JsonBuilder()

        gb {
            vertices vbs
            edges ebs
        }

        def f = new File(".latch/dag.json")
        f.write(gb.toPrettyString())
    }
}
