package nextflow.dagGeneration

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicLong

import groovy.json.JsonBuilder
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.latch.LatchUtils
import org.codehaus.groovy.ast.ASTNode
import org.codehaus.groovy.ast.ClassCodeVisitorSupport
import org.codehaus.groovy.ast.ClassNode
import org.codehaus.groovy.ast.ImportNode
import org.codehaus.groovy.ast.ModuleNode
import org.codehaus.groovy.ast.expr.ArgumentListExpression
import org.codehaus.groovy.ast.expr.BinaryExpression
import org.codehaus.groovy.ast.expr.BooleanExpression
import org.codehaus.groovy.ast.expr.CastExpression
import org.codehaus.groovy.ast.expr.ClosureExpression
import org.codehaus.groovy.ast.expr.ConstantExpression
import org.codehaus.groovy.ast.expr.Expression
import org.codehaus.groovy.ast.expr.MethodCallExpression
import org.codehaus.groovy.ast.expr.PropertyExpression
import org.codehaus.groovy.ast.expr.TupleExpression
import org.codehaus.groovy.ast.expr.VariableExpression
import org.codehaus.groovy.ast.stmt.BlockStatement
import org.codehaus.groovy.ast.stmt.ExpressionStatement
import org.codehaus.groovy.ast.stmt.IfStatement
import org.codehaus.groovy.ast.stmt.Statement
import org.codehaus.groovy.control.CompilePhase
import org.codehaus.groovy.control.SourceUnit
import org.codehaus.groovy.syntax.Types
import org.codehaus.groovy.transform.ASTTransformation
import org.codehaus.groovy.transform.GroovyASTTransformation
import org.iq80.leveldb.table.Block

@Slf4j
//@CompileStatic
@GroovyASTTransformation(phase = CompilePhase.CONVERSION)
class DAGGeneratorImpl implements ASTTransformation {

    static class DAGGenerationException extends Exception {
        DAGGenerationException(String msg) {
            super(msg)
        }
    }

    static class Vertex {
        static private AtomicLong nextID = new AtomicLong()

        static enum Type {
            Process,
            Operator,
            SubWorkflow,
            // Internal
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

    static class ConditionalVertex extends Vertex {
        BooleanExpression condition

        ConditionalVertex( String label, BooleanExpression condition ) {
            super(Type.Conditional, label)

            this.condition = condition
        }
    }

    static class ProcessVertex extends Vertex {
        public MethodCallExpression definition
        public MethodCallExpression call

        ProcessVertex (
            String label,
            MethodCallExpression definition,
            MethodCallExpression call
        ) {
            super(Type.Process, label)

            this.definition = definition
            this.call = call
        }
    }

    static class Edge {
        String label
        Vertex from
        Vertex to

        Edge( String label, Vertex from, Vertex to ) {
            this.label = label
            this.from = from
            this.to = to
        }
    }

    static class ConditionalEdge extends Edge {

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

//    @CompileStatic
    class DAGBuilder {

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

        String workflowName
        Map<String, NFEntity> scope

        DAGBuilder(String workflowName, Map<String, NFEntity> scope) {
            this.workflowName = workflowName
            this.scope = scope
        }

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

                        producers[v.name] = res
                    } else if (expr.leftExpression instanceof TupleExpression) {
                        def t = expr.leftExpression as TupleExpression

                        for (def sub: t.expressions) {
                            def v = sub as VariableExpression
                            producers[v.name] = res
                        }
                    } else {
                        throw new DAGGenerationException("Cannot handle left expression in binary expression of type $expr.leftExpression.class: $expr.leftExpression.text ")
                    }
                    break
                case Types.PIPE:
                    def method = expr.rightExpression
                    def arg = expr.leftExpression

                    return visitMethodCallExpression(
                        new MethodCallExpression(
                            new ConstantExpression("this"),
                            method,
                            new ArgumentListExpression([arg])
                        )
                    )
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

            def entity = this.scope.get(expr.methodAsString)

            Vertex v = null
            if (entity?.type == NFEntity.Type.Process) {
                v = new ProcessVertex(expr.methodAsString, entity.definition, expr)
                producers[expr.methodAsString] = v // allow processName.out calls
            } else if (entity?.type == NFEntity.Type.Workflow) {
                v = new Vertex(Vertex.Type.SubWorkflow, expr.methodAsString)
                producers[expr.methodAsString] = v // allow workflowName.out calls
            } else if (expr.methodAsString in this.operatorNames) {
                v = new Vertex(Vertex.Type.Operator, expr.methodAsString)
            } else if (expr.objectExpression.text == "Channel" && expr.methodAsString in this.channelFactories) {
                v = new Vertex(Vertex.Type.Generator, expr.text)
            }

            if (expr.methodAsString == "set") {
                if (objProducer == null)
                    return null

                def lst = (ArgumentListExpression) expr.arguments

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
            if (!(expr.arguments instanceof ArgumentListExpression)) {
                // handles operators which take in records, e.g. collectFile()
                return v
            }

            def lst = (ArgumentListExpression) expr.arguments
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

        Vertex visitPropertyExpression(PropertyExpression expr) {
            return visitExpression(expr.objectExpression)
        }


        Vertex visitExpression(Expression expr) {
            Vertex res
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
                case PropertyExpression:
                    res = visitPropertyExpression(expr)
                    break

                case ConstantExpression:
                default:
                    res = null
            }

            log.debug "${expr.class}: $expr.text --> $res"
            return res
        }

        void addVertex(Vertex v) {
            this.vertices.add(v)

            this.activeConditionals.forEach {cond, branch ->
                this.edges.add(new ConditionalEdge(cond.condition.text, cond, v, branch))
            }
        }

        void visitStatement(Statement s) {
            switch (s) {
                case ExpressionStatement:
                    visitExpression(s.expression)
                    break
                case IfStatement:
                    def stmt = s as IfStatement

                    def bool = stmt.booleanExpression
                    def cond = new ConditionalVertex(bool.text, bool)
                    addVertex(cond)

                    def condProducer = visitExpression(bool)
                    if (condProducer != null) {
                        def e = new Edge(condProducer.label, condProducer, cond);
                        edges.add(e)
                    }

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
                Map<String, String> processMeta = null
                if (v instanceof ProcessVertex) {
                    processMeta = [call: v.call.text]
                }

                def vb = new JsonBuilder([
                    id: v.id.toString(),
                    label: v.label,
                    type: v.type,
                    processMeta: processMeta,
                ])

                vbs.add(vb)
            }

            for (Edge e: this.edges) {
                Boolean branch = null
                if (e instanceof ConditionalEdge) {
                    branch = e.branch
                }

                def eb = new JsonBuilder([
                    label: e.label,
                    src: e.from.id.toString(),
                    dest: e.to.id.toString(),
                    branch: branch,
                ])

                ebs.add(eb)
            }

            def gb = new JsonBuilder([
                vertices: vbs,
                edges: ebs,
            ])

            def f = new File(".latch/${workflowName}.dag.json")
            f.write(gb.toPrettyString())
        }
    }

    @CompileStatic
    class Visitor extends ClassCodeVisitorSupport {
        SourceUnit unit
        String currentImportPath
        Map<String, NFEntity> scope = new LinkedHashMap()

        Visitor(SourceUnit unit) {
            this.unit = unit
        }

        SourceUnit getSourceUnit() {
            this.unit
        }

        void addToScope(Expression expr) {
            Map<String, NFEntity> definingScope = scopes[currentImportPath]
            if (definingScope == null) {
                log.error "Attempted to include module before it was parsed"
                log.error "keys: ${scopes.keySet()}"
                log.error "looking for: ${currentImportPath}"
                return
            }

            if (expr instanceof ConstantExpression) {
                scope[expr.text] = definingScope[expr.text]
            } else if (expr instanceof VariableExpression) {
                scope[expr.name] = definingScope[expr.name]
            } else if (expr instanceof CastExpression && expr.expression instanceof VariableExpression) {
                def v = expr.expression as VariableExpression
                scope[v.name] = definingScope[expr.type.name]
            } else {
                log.error "Malformed include statement"
            }
        }

//        Map<String, List<String>> getProcessIO(MethodCallExpression expr) {
//            def args = expr.arguments as ArgumentListExpression
//            if (!args[0] instanceof ClosureExpression) return null;
//
//            def closure = args[0] as ClosureExpression
//            def code = closure.code as BlockStatement
//
//            def context = null;
//            for (def stmt: code.statements) {
//                context = stmt.statementLabel ?: context
//
//                switch (context) {
//                    case "":
//                }
//            }
//        }

        @Override
        void visitMethodCallExpression(MethodCallExpression expr) {

            final preCondition = expr.objectExpression?.getText() == 'this'

            if (expr.methodAsString == "process" && preCondition) {
                def args = expr.arguments as ArgumentListExpression
                if (!args[0] instanceof MethodCallExpression) {
                    log.debug "${args[0].class}: ${args[0].text}"
                }
                def sub = args[0] as MethodCallExpression

                scope[sub.methodAsString] = new NFEntity(NFEntity.Type.Process, expr)

                // todo(ayush): record process inputs/outputs
            } else if (expr.methodAsString == "workflow") {
                visitWorkflowDef(expr, scope)
            } else if (expr.text.startsWith("this.include")) {
                def args = (ArgumentListExpression) expr.arguments

                if (args.size() != 1) {
                    log.error "Malformed include statement"
                    return
                }

                def arg = args[0]
                if (expr.methodAsString == "from") {
                    if (!(arg instanceof ConstantExpression)) {
                        log.error "Malformed include statement"
                        return
                    }

                    def c = arg as ConstantExpression
                    if (c.text.startsWith("plugin")) {
                        return
                    }

                    def parent = Path.of(this.unit.name).parent
                    currentImportPath = "${Path.of(parent.toString(), c.text).normalize()}.nf"
                } else if (arg instanceof ClosureExpression) {
                    def closure = arg as ClosureExpression
                    def code = closure.code as BlockStatement

                    for (def stmt: code.statements) {
                        if (!(stmt instanceof ExpressionStatement)) {
                            log.error "Malformed include statement"
                            return
                        }

                        addToScope(stmt.expression)
                    }
                } else {
                    addToScope(arg)
                }
            }

            super.visitMethodCallExpression(expr)
        }
    }

    Set<String> workflowNames = new HashSet()
    int anonymousWorkflows = 0
    Map<String, DAGBuilder> dags = new LinkedHashMap()

    static class NFEntity {
        static enum Type {
            Process,
            Workflow
        }

        public Type type
        public MethodCallExpression definition

        public List<String> inputs
        public List<String> outputs

        NFEntity( Type type, MethodCallExpression definition ) {
            this.type = type
            this.definition = definition

//            this.inputs = inputs
//            this.outputs = outputs
        }
    }

    Map<String, Map<String, NFEntity>> scopes = new LinkedHashMap();

    // Errors in the script like typos, etc. are handled in other AST transforms
    void visitWorkflowDef(MethodCallExpression expr, Map<String, NFEntity> currentScope) {
        assert expr.arguments instanceof ArgumentListExpression
        def args = (ArgumentListExpression)expr.arguments

        if (args.size() != 1) {
            log.debug "Malformed workflow definition at line: ${expr.lineNumber}"
            return
        }

        // anonymous workflows
        String name = "mainWorkflow"
        ClosureExpression closure
        if( args[0] instanceof ClosureExpression ) {
            if( anonymousWorkflows++ > 0 )
                return

            closure = args[0] as ClosureExpression
        } else {
            // extract the first argument which has to be a method-call expression
            // the name of this method represent the *workflow* name
            final nested = args[0] as MethodCallExpression
            name = nested.getMethodAsString()


            def subargs = nested.arguments  as ArgumentListExpression
            closure = subargs[0] as ClosureExpression
        }

        currentScope[name] = new NFEntity(NFEntity.Type.Workflow, expr)
        workflowNames.add(name)

        def body = new ArrayList<Statement>()
        def input = new ArrayList<Statement>()
        def output = new ArrayList<Statement>()
        String context = null

        final codeBlock = closure.code as BlockStatement
        for ( Statement x : codeBlock.statements ) {
            context = x.statementLabel ?: context

            switch (context) {
                case 'take':
                    input.add(x)
                    break
                case 'emit':
                    output.add(x) // todo(ayush): do something with this
                    break
                case 'main':
                default:
                    body.add(x)
            }
        }

        def dagBuilder = new DAGBuilder(name, currentScope)

        for (def s: input) dagBuilder.addInputNode(s)
        for (def s: body) dagBuilder.visit(s)

        dags[name] = dagBuilder
    }


    @Override
    void visit(ASTNode[] nodes, SourceUnit source) {
        def v = new Visitor(source)
        v.visitClass((ClassNode) nodes[1])
        scopes[source.name] = v.scope

        this.dags.forEach {_, dag -> dag.writeDAG()}
    }
}
