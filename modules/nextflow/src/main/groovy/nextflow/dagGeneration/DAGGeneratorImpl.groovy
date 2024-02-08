package nextflow.dagGeneration

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicLong

import groovy.json.JsonBuilder
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.codehaus.groovy.ast.ASTNode
import org.codehaus.groovy.ast.ClassCodeVisitorSupport
import org.codehaus.groovy.ast.ClassNode
import org.codehaus.groovy.ast.Parameter
import org.codehaus.groovy.ast.VariableScope
import org.codehaus.groovy.ast.expr.ArgumentListExpression
import org.codehaus.groovy.ast.expr.BinaryExpression
import org.codehaus.groovy.ast.expr.BooleanExpression
import org.codehaus.groovy.ast.expr.CastExpression
import org.codehaus.groovy.ast.expr.ClosureExpression
import org.codehaus.groovy.ast.expr.ConstantExpression
import org.codehaus.groovy.ast.expr.Expression
import org.codehaus.groovy.ast.expr.GStringExpression
import org.codehaus.groovy.ast.expr.ListExpression
import org.codehaus.groovy.ast.expr.MapEntryExpression
import org.codehaus.groovy.ast.expr.MapExpression
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
import org.codehaus.groovy.syntax.Token
import org.codehaus.groovy.syntax.Types
import org.codehaus.groovy.transform.ASTTransformation
import org.codehaus.groovy.transform.GroovyASTTransformation
import static nextflow.ast.ASTHelpers.isBinaryX
import static nextflow.ast.ASTHelpers.isConstX
import static nextflow.ast.ASTHelpers.isMapX
import static nextflow.ast.ASTHelpers.isStmtX
import static nextflow.ast.ASTHelpers.isTupleX
import static nextflow.ast.ASTHelpers.isVariableX
import static org.codehaus.groovy.ast.tools.GeneralUtils.constX

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
    }

    static class ConditionalVertex extends Vertex {
        ConditionalVertex( String label, Statement stmt, List<Statement> ret ) {
            super(Type.Conditional, label, stmt, ret)
        }
    }

    static class ProcessVertex extends Vertex {
        ProcessVertex ( String label, Statement stmt, List<Statement> ret, List<String> outputNames, String module, String unaliased ) {
            super(Type.Process, label, stmt, ret)

            this.outputNames = outputNames
            this.module = module
            this.unaliased = unaliased
        }
    }

    static class SubWorkflowVertex extends Vertex {
        SubWorkflowVertex ( String label, Statement stmt, List<Statement> ret, List<String> outputNames ) {
            super(Type.SubWorkflow, label, stmt, ret)

            this.outputNames = outputNames
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
        final Set<String> specialOperatorNames = new HashSet(["branch", "multiMap"])

        String workflowName
        String module = ""
        Map<String, NFEntity> scope

        DAGBuilder(String workflowName, Map<String, NFEntity> scope, String module) {
            this.workflowName = workflowName
            this.scope = scope
            this.module = module
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
            // - a comp b for comp in [<, >, <=, >=]

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
                    def left = visitExpression(expr.leftExpression)
                    def right = visitExpression(expr.rightExpression)

                    // todo(ayush): this sucks
                    Expression call
                    if ((left == null) && (right == null)) {
                        call = expr
                    } else {
                        Expression leftExpr
                        if (left == null) {
                            if (expr.leftExpression.text.startsWith("Channel")) {
                                // Channel.of(...)
                                leftExpr = expr.leftExpression
                            } else {
                                leftExpr = new MethodCallExpression(
                                    new VariableExpression("Channel"),
                                    "value",
                                    new ArgumentListExpression([expr.leftExpression])
                                )
                            }
                        } else {
                            leftExpr = new MethodCallExpression(
                                new VariableExpression("Channel"),
                                "of",
                                new ArgumentListExpression([])
                            )
                        }

                        Expression rightExpr
                        if (right == null) {
                            if (expr.rightExpression.text.startsWith("Channel")) {
                                // Channel.of(...)
                                rightExpr = expr.rightExpression
                            } else {
                                rightExpr = new MethodCallExpression(
                                    new VariableExpression("Channel"),
                                    "value",
                                    new ArgumentListExpression([expr.rightExpression])
                                )
                            }
                        } else {
                            rightExpr = new MethodCallExpression(
                                new VariableExpression("Channel"),
                                "of",
                                new ArgumentListExpression([])
                            )
                        }

                        call = new MethodCallExpression(
                            new MethodCallExpression(
                                leftExpr,
                                "combine",
                                new ArgumentListExpression([rightExpr])
                            ),
                            "map",
                            new ArgumentListExpression([
                                new ClosureExpression(
                                    [] as Parameter[],
                                    new BlockStatement(
                                        [
                                            new ExpressionStatement(
                                                new BinaryExpression(
                                                    new BinaryExpression(
                                                        new VariableExpression("it"),
                                                        Token.newSymbol("[", -1, -1),
                                                        new ConstantExpression(0)
                                                    ),
                                                    expr.operation,
                                                    new BinaryExpression(
                                                        new VariableExpression("it"),
                                                        Token.newSymbol("[", -1, -1),
                                                        new ConstantExpression(1)
                                                    ),
                                                )
                                            )
                                        ],
                                        new VariableScope(),
                                    )
                                )
                            ])
                        )
                    }

                    def v = new Vertex(
                        Vertex.Type.Generator,
                        expr.text,
                        new ExpressionStatement(
                            new BinaryExpression(
                                new VariableExpression("res"),
                                Token.newSymbol("=", 0, 0),
                                call
                            )
                        ),
                        [new ExpressionStatement(new VariableExpression("res"))]
                    )

                    addVertex(v)
                    if (left != null)
                        edges.add(new Edge(left.label, left, v))
                    if (right != null)
                        edges.add(new Edge(right.label, right, v))

                    return v
            }
        }

        Vertex visitMethodCallExpression (MethodCallExpression expr) {
            // a method call expression should add either an operator node or a process node,
            // exceptions:
            // - Anonymous channel factories like Channel.of etc.

            Map<String, Vertex> dependencies = [:]
            def objProducer = visitExpression(expr.objectExpression)
            if (objProducer != null)
                dependencies[expr.objectExpression.text] = objProducer

            if (expr.arguments instanceof ArgumentListExpression) {
                for (def x: (ArgumentListExpression) expr.arguments) {
                    def producer = visitExpression(x)
                    if (producer == null) continue

                    dependencies[x.text] = producer
                }
            }

            def entity = this.scope.get(expr.methodAsString)

            Vertex v = null
            if (entity?.type == NFEntity.Type.Process) {
                def module = ""
                def unaliased = ""
                if (entity.module != this.module) {
                    module = entity.module
                    unaliased = entity.unaliased
                }

                v = new ProcessVertex(
                    expr.methodAsString,
                    new ExpressionStatement(
                        new BinaryExpression(
                            new VariableExpression("res"),
                            Token.newSymbol("=", 0, 0),
                            new MethodCallExpression(
                                new VariableExpression('this'),
                                new ConstantExpression(expr.methodAsString),
                                new ArgumentListExpression([] as List<Expression>)
                            )
                        )
                    ),
                    [new ExpressionStatement(new VariableExpression("res"))],
                    entity.outputs,
                    module,
                    unaliased
                )

                // allow processName.out calls
                producers[expr.methodAsString] = v

            } else if (entity?.type == NFEntity.Type.Workflow) {
                // todo(ayush): update subworkflow body too
                v = new SubWorkflowVertex(
                    expr.methodAsString,
                    new ExpressionStatement(
                        new BinaryExpression(
                            new VariableExpression("res"),
                            Token.newSymbol("=", 0, 0),
                            new MethodCallExpression(
                                new VariableExpression('this'),
                                new ConstantExpression(expr.methodAsString),
                                new ArgumentListExpression([] as List<Expression>)
                            )
                        )
                    ),
                    [new ExpressionStatement(new VariableExpression("res"))],
                    entity.outputs
                )

                producers[expr.methodAsString] = v // allow workflowName.out calls
            } else if (expr.methodAsString in this.operatorNames) {
                def ret = [new ExpressionStatement(new VariableExpression("res"))]

                List<String> outputNames = [];
                if (expr.methodAsString in this.specialOperatorNames) {

                    def args = expr.arguments as ArgumentListExpression
                    def closure = args.expressions[0] as ClosureExpression
                    def block = closure.code as BlockStatement

                    for (def x: block.statements) {
                        for (def label: x.statementLabels) {
                            outputNames.add(label)
                        }
                    }

                    ret = outputNames.collect {
                        new ExpressionStatement(
                            new BinaryExpression(
                                new VariableExpression(it),
                                Token.newSymbol("=", 0, 0),
                                new PropertyExpression(
                                    new VariableExpression("res"),
                                    it
                                )
                            )
                        )
                    }
                }

                v = new Vertex(
                    Vertex.Type.Operator,
                    expr.methodAsString,
                    new ExpressionStatement(
                        new BinaryExpression(
                            new VariableExpression("res"),
                            Token.newSymbol("=", 0, 0),
                            new MethodCallExpression(
                                new MethodCallExpression(
                                    new VariableExpression('Channel'),
                                    "of",
                                    new ArgumentListExpression([])
                                ),
                                new ConstantExpression(expr.methodAsString),
                                expr.arguments,
                            )
                        )
                    ),
                    ret
                )

                v.outputNames = outputNames
            } else if (expr.objectExpression.text == "Channel" && expr.methodAsString in this.channelFactories) {
                v = new Vertex(
                    Vertex.Type.Generator,
                    expr.text,
                    new ExpressionStatement(
                        new BinaryExpression(
                            new VariableExpression("res"),
                            Token.newSymbol("=", 0, 0),
                            expr
                        )
                    ),
                    [new ExpressionStatement(new VariableExpression("res"))]
                )
            } else if (expr.methodAsString == "set") {
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
                for (def x: dependencies) {
                    if (x == null)
                        continue

                    edges.add(new Edge(x.key, x.value, v))
                }
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

        Vertex visitListExpression(ListExpression expr) {
            List<Vertex> producers = []

            List<Expression> transformed = expr.expressions.collect {
                def producer = visitExpression(it)
                producers << producer

                if (producer == null) {
                    return it
                }

                return new MethodCallExpression(
                    new VariableExpression("Channel"),
                    "of",
                    new ArgumentListExpression([])
                )
            }

            def v = new Vertex(
                Vertex.Type.Generator,
                expr.text,
                new ExpressionStatement(
                    new BinaryExpression(
                        new VariableExpression("res"),
                        Token.newSymbol("=", -1, -1),
                        new ListExpression(transformed)
                    )
                ),
                [new ExpressionStatement(new VariableExpression("res"))]
            )

            addVertex(v)
            producers.collect {
                if (it == null) return

                edges.add(new Edge(it.label, it, v))
                return
            }

            return v
        }


        Vertex visitMapExpression(MapExpression expr) {
            List<Vertex> producers = []

            List<MapEntryExpression> transformed = expr.mapEntryExpressions.collect {
                def keyProducer = visitExpression(it.keyExpression)
                def valProducer = visitExpression(it.valueExpression)
                producers << keyProducer
                producers << valProducer

                Expression keyExpr = it.keyExpression
                if (keyProducer != null) {
                    keyExpr = new MethodCallExpression(
                        new VariableExpression("Channel"),
                        "of",
                        new ArgumentListExpression([])
                    )
                }

                Expression valExpr = it.valueExpression
                if (valProducer != null) {
                    valExpr = new MethodCallExpression(
                        new VariableExpression("Channel"),
                        "of",
                        new ArgumentListExpression([])
                    )
                }

                return new MapEntryExpression(keyExpr, valExpr)
            }

            def v = new Vertex(
                Vertex.Type.Generator,
                expr.text,
                new ExpressionStatement(
                    new BinaryExpression(
                        new VariableExpression("res"),
                        Token.newSymbol("=", -1, -1),
                        new MapExpression(transformed)
                    )
                ),
                [new ExpressionStatement(new VariableExpression("res"))]
            )

            addVertex(v)
            producers.collect {
                if (it == null) return

                edges.add(new Edge(it.label, it, v))
                return
            }

            return v
        }


        Vertex visitGStringExpression(GStringExpression expr) {
            List<Vertex> producers = []

            List<Expression> transformed = expr.values.collect {
                def producer = visitExpression(it)
                producers << producer

                if (producer == null) {
                    return it
                }

                return new MethodCallExpression(
                    new VariableExpression("Channel"),
                    "of",
                    new ArgumentListExpression([])
                )
            }

            def v = new Vertex(
                Vertex.Type.Generator,
                expr.text,
                new ExpressionStatement(
                    new BinaryExpression(
                        new VariableExpression("res"),
                        Token.newSymbol("=", -1, -1),
                        new GStringExpression(expr.text, expr.strings, transformed)
                    )
                ),
                [new ExpressionStatement(new VariableExpression("res"))]
            )

            addVertex(v)
            producers.collect {
                if (it == null) return

                edges.add(new Edge(it.label, it, v))
                return
            }

            return v
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
                case BooleanExpression:
                    res = visitExpression(expr.expression)
                    break
                case ListExpression:
                    res = visitListExpression(expr)
                    break
                case MapExpression:
                    res = visitMapExpression(expr)
                    break
                case GStringExpression:
                    res = visitGStringExpression(expr)
                    break
                case ConstantExpression:
                    res = null
                    break
                default:
                    throw new DAGGenerationException("Cannot process expression of type ${expr?.class}")
            }

            log.debug "${expr.class}: $expr.text --> $res"
            return res
        }

        void addVertex(Vertex v) {
            this.vertices.add(v)

            this.activeConditionals.forEach {cond, branch ->
                this.edges.add(new ConditionalEdge(cond.call.text, cond, v, branch))
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
                    def condProducer = visitExpression(bool)

                    def sub = bool
                    if (condProducer != null) {
                        sub = new MethodCallExpression(
                            new VariableExpression("Channel"),
                            "of",
                            new ArgumentListExpression([])
                        )
                    }

                    def cond = new ConditionalVertex(
                        bool.text,
                        new ExpressionStatement(
                            new BinaryExpression(
                                new VariableExpression("res"),
                                Token.newSymbol("=", 0, 0),
                                sub,
                            )
                        ),
                        [new ExpressionStatement(new VariableExpression("res"))]
                    )
                    addVertex(cond)

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
                    def v = new Vertex(Vertex.Type.Input, expr.name, null, null)
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
                def vb = new JsonBuilder([
                    id: v.id.toString(),
                    label: v.label,
                    type: v.type,
                    statement: v.call != null ? StatementJSONConverter.toJsonString(v.call) : null,
                    ret: v.ret != null ? v.ret.collect {StatementJSONConverter.toJsonString(it)} : null,
                    outputNames: v.outputNames,
                    module: v.module,
                    unaliased: v.unaliased
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

        void visitProcessDef(MethodCallExpression expr) {
            def _args = expr.arguments as ArgumentListExpression
            def sub = _args[0] as MethodCallExpression

            def processName = sub.methodAsString

            def closure = ((ArgumentListExpression) sub.arguments)[0] as ClosureExpression

            List<String> outputNames = []

            String context = null
            for (Statement stmt: ((BlockStatement) closure.code).statements) {
                context = stmt.statementLabel ?: context
                if (context != "output")
                    continue

                if (!(stmt instanceof ExpressionStatement) || !((stmt as ExpressionStatement).expression instanceof MethodCallExpression))
                    continue

                MethodCallExpression call = (stmt as ExpressionStatement).expression as MethodCallExpression

                List<Expression> args = isTupleX(call.arguments)?.expressions
                if (args == null) continue

                if (args.size() < 2 && (args.size() != 1 || call.methodAsString != "stdout")) return

                for (def arg: args) {
                    MapExpression map = isMapX(arg)
                    if (map == null) continue

                    for (int i = 0; i < map.mapEntryExpressions.size(); i++) {
                        final entry = map.mapEntryExpressions[i]
                        final key = isConstX(entry.keyExpression)
                        final val = isVariableX(entry.valueExpression)

                        if( key?.text == 'emit' && val != null ) {
                            outputNames << val.text
                            break
                        }
                    }
                }
            }

            scope[processName] = new NFEntity(NFEntity.Type.Process, expr, outputNames, sourceUnit.name, processName)
        }

        @Override
        void visitMethodCallExpression(MethodCallExpression expr) {

            final preCondition = expr.objectExpression?.getText() == 'this'

            if (expr.methodAsString == "process" && preCondition) {
                visitProcessDef(expr)
            } else if (expr.methodAsString == "workflow") {
                visitWorkflowDef(expr, scope, sourceUnit.name)
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

    Map<String, Map<String, NFEntity>> scopes = new LinkedHashMap();

    // Errors in the script like typos, etc. are handled in other AST transforms
    void visitWorkflowDef(MethodCallExpression expr, Map<String, NFEntity> currentScope, String module) {
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

        def dagBuilder = new DAGBuilder(name, currentScope, module)

        for (def s: input) dagBuilder.addInputNode(s)
        for (def s: body) dagBuilder.visit(s)

        dags[name] = dagBuilder

        List<String> outputNames = []
        for (def s: output) {
            ExpressionStatement es = isStmtX(s)
            if (es == null) continue

            def be = isBinaryX(es.expression)
            def v = isVariableX(es.expression)

            if (be != null) {
                outputNames << be.leftExpression.text
            } else if (v != null) {
                outputNames << v.text
            }
        }

        currentScope[name] = new NFEntity(NFEntity.Type.Workflow, expr, outputNames, module, name)
        workflowNames.add(name)
    }


    @Override
    void visit(ASTNode[] nodes, SourceUnit source) {
        def v = new Visitor(source)
        v.visitClass((ClassNode) nodes[1])
        scopes[source.name] = v.scope

        this.dags.forEach {_, dag -> dag.writeDAG()}
    }
}
