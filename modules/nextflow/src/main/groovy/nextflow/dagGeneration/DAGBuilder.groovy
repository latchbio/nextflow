package nextflow.dagGeneration

import groovy.json.JsonBuilder
import groovy.util.logging.Slf4j
import nextflow.ast.ASTHelpers
import org.codehaus.groovy.ast.Parameter
import org.codehaus.groovy.ast.VariableScope
import org.codehaus.groovy.ast.expr.ArgumentListExpression
import org.codehaus.groovy.ast.expr.BinaryExpression
import org.codehaus.groovy.ast.expr.BooleanExpression
import org.codehaus.groovy.ast.expr.ClosureExpression
import org.codehaus.groovy.ast.expr.ConstantExpression
import org.codehaus.groovy.ast.expr.Expression
import org.codehaus.groovy.ast.expr.GStringExpression
import org.codehaus.groovy.ast.expr.ListExpression
import org.codehaus.groovy.ast.expr.MapEntryExpression
import org.codehaus.groovy.ast.expr.MapExpression
import org.codehaus.groovy.ast.expr.MethodCallExpression
import org.codehaus.groovy.ast.expr.PropertyExpression
import org.codehaus.groovy.ast.expr.StaticMethodCallExpression
import org.codehaus.groovy.ast.expr.TupleExpression
import org.codehaus.groovy.ast.expr.VariableExpression
import org.codehaus.groovy.ast.stmt.BlockStatement
import org.codehaus.groovy.ast.stmt.ExpressionStatement
import org.codehaus.groovy.ast.stmt.IfStatement
import org.codehaus.groovy.ast.stmt.Statement
import org.codehaus.groovy.syntax.Token
import org.codehaus.groovy.syntax.Types

@Slf4j
class DAGBuilder {
    static class DAGGenerationException extends Exception {
        DAGGenerationException(String msg) {
            super(msg)
        }
    }

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
        "branch",
        "buffer",
        "collate",
        "collect",
        "collectFile",
        "combine",
        "concat",
        "count",
        "countFasta",
        "countFastq",
        "countJson",
        "countLines",
        "cross",
        "distinct",
        "dump",
        "filter",
        "first",
        "flatMap",
        "flatten",
        "groupTuple",
        "ifEmpty",
        "join",
        "last",
        "map",
        "max",
        "merge",
        "min",
        "mix",
        "multiMap",
        "randomSample",
        "reduce",
        "splitCsv",
        "splitFasta",
        "splitFastq",
        "splitJson",
        "splitText",
        "subscribe",
        "sum",
        "take",
        "tap",
        "toInteger",
        "toList",
        "toSortedList",
        "transpose",
        "unique",
        "until",
        "view",
        // "set" is excluded on purpose as it does not warrant adding a vertex to the graph
    ])

    final Set<String> specialOperatorNames = new HashSet(["branch", "multiMap"])

    String workflowName
    String module = ""
    // process / workflow names
    Map<String, NFEntity> entities
    DAGGeneratorImpl generator
    Scope scope

    DAGBuilder(String workflowName, Map<String, NFEntity> entities, String module, DAGGeneratorImpl generator) {
        this.workflowName = workflowName
        this.entities = entities
        this.module = module
        this.generator = generator
        this.scope = new Scope()
    }

    List<Edge> edges = new ArrayList<>()
    List<Vertex> vertices = new ArrayList<>()
    List<Vertex> inputNodes = new ArrayList<>()


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
        // - a | b --> b(a())
        // - a op b for op in [+, -, *, /, %, **, ^]

        switch (expr.operation.type) {
            case Types.EQUAL:
                // assignment should update producers

                def res = visitExpressionWithLiteralCheck(expr.rightExpression)

                if (expr.leftExpression instanceof VariableExpression) {
                    def v = expr.leftExpression as VariableExpression

                    scope.set(v.name, res)
                } else if (expr.leftExpression instanceof TupleExpression) {
                    def t = expr.leftExpression as TupleExpression

                    // todo(ayush): figure out how to distinguish edges here
                    for (def sub: t.expressions) {
                        def v = sub as VariableExpression
                        scope.set(v.name, res)
                    }
                } else {
                    throw new DAGGenerationException("Cannot handle left expression in binary expression of type $expr.leftExpression.class: $expr.leftExpression.text ")
                }

                return null
            case Types.PIPE:
                def method = expr.rightExpression
                def arg = expr.leftExpression

                if (arg instanceof VariableExpression) {
                    // proc_1 | proc_2 => proc_2(proc_1()), not proc_2(proc_1)

                    arg = new MethodCallExpression(
                        new ConstantExpression("this"),
                        arg.text,
                        new ArgumentListExpression([])
                    )
                }

                return visitMethodCallExpression(
                    new MethodCallExpression(
                        new ConstantExpression("this"),
                        method.text, // superhack
                        new ArgumentListExpression([arg])
                    )
                )
            case Types.BITWISE_AND:
                throw new DAGGenerationException("'&' operator is not currently supported")
            default:
                def left = visitExpression(expr.leftExpression)
                def right = visitExpression(expr.rightExpression)

                // todo(ayush): this sucks
                Expression call
                if ((left == null) && (right == null)) {
                    call = expr
                } else if (left == null) {
                    call = new MethodCallExpression(
                        new MethodCallExpression(
                            new VariableExpression("Channel"),
                            "of",
                            new ArgumentListExpression([])
                        ),
                        "binaryOp",
                        new ArgumentListExpression([
                            expr.leftExpression,
                            new ConstantExpression(expr.operation.text),
                            new ConstantExpression(true)
                        ])
                    )
                } else {
                    Expression rightExpr = right == null
                        ? expr.rightExpression
                        : new MethodCallExpression(
                            new VariableExpression("Channel"),
                            "of",
                            new ArgumentListExpression([])
                        )

                    call = new MethodCallExpression(
                        new MethodCallExpression(
                            new VariableExpression("Channel"),
                            "of",
                            new ArgumentListExpression([])
                        ),
                        "binaryOp",
                        new ArgumentListExpression([
                            rightExpr,
                            new ConstantExpression(expr.operation.text)
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
        // a method call expression should add either
        // - an operator node
        // - a process node
        // - a suworkflow node
        // - a generator node (anonymous channel factories e.g.)

        if (expr.methodAsString == "set") {
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

            return visitBinaryExpression(new BinaryExpression(ve, Token.newSymbol("=", -1, -1), expr.objectExpression))
        }

        Map<String, Vertex> dependencies = [:]
        def objProducer = visitExpression(expr.objectExpression)
        if (objProducer != null)
            dependencies[expr.objectExpression.text] = objProducer

        List<Expression> newArgs = []
        if (expr.arguments instanceof ArgumentListExpression) {
            for (def x: (ArgumentListExpression) expr.arguments) {
                def producer = visitExpression(x)
                if (producer == null) {
                    newArgs.add(x)
                    continue
                }

                newArgs.add(
                    new MethodCallExpression(
                        new VariableExpression('Channel'),
                        "of",
                        new ArgumentListExpression([])
                    )
                )

                dependencies[x.text] = producer
            }
        }

        def entity = this.entities.get(expr.methodAsString)

        Vertex v = null
        if (entity?.type == NFEntity.Type.Process) {
            List<Statement> ret
            if (entity.outputs.size() > 0) {
                ret = entity.outputs.collect {
                    new ExpressionStatement(
                        new BinaryExpression(
                            new VariableExpression(it),
                            Token.newSymbol("=", 0, 0),
                            new PropertyExpression(
                                new PropertyExpression(
                                    new VariableExpression(expr.methodAsString),
                                    "out"
                                ),
                                it
                            )
                        )
                    )
                }
            } else {
                ret = [
                    new ExpressionStatement(
                        new BinaryExpression(
                            new VariableExpression("res"),
                            Token.newSymbol("=", 0, 0),
                            new PropertyExpression(
                                new VariableExpression(expr.methodAsString),
                                "out"
                            )
                        )
                    )
                ]
            }

            v = new ProcessVertex(
                expr.methodAsString,
                new ExpressionStatement(
                    new MethodCallExpression(
                        new VariableExpression('this'),
                        new ConstantExpression(expr.methodAsString),
                        new ArgumentListExpression([] as List<Expression>)
                    )
                ),
                ret,
                entity.outputs,
                entity.module,
                entity.unaliased
            )

            // todo(ayush): enable this always to support tuple unpacking
            if (entity.outputs.size() == 0) {
                scope.set("${expr.methodAsString}.out", v)
            }

            for (String o: entity.outputs) {
                scope.set("${expr.methodAsString}.out.$o", v)
            }

        } else if (entity?.type == NFEntity.Type.Workflow) {
            List<Vertex> inputs = []
            DAGBuilder subWorkflowDag = generator.dags[entity.unaliased]
            Map<Vertex, Vertex> idMapping = new HashMap<Vertex, Vertex>();

            for (def _subV: subWorkflowDag.vertices) {
                // maintain distinct IDs (only a problem if a subworkflow is called more than once)
                def subV = Vertex.clone(_subV)
                idMapping[_subV] = subV

                if (subV.type == Vertex.Type.Input) {
                    inputs << subV
                }

                addVertex(subV)
            }

            for (def _subE: subWorkflowDag.edges) {
                def from = idMapping[_subE.from]
                def to = idMapping[_subE.to]

                Edge subE
                if (_subE instanceof ConditionalEdge) {
                    subE = new ConditionalEdge(
                        _subE.label,
                        from,
                        to,
                        _subE.branch,
                    )
                } else {
                    subE = new Edge(
                        _subE.label,
                        from,
                        to,
                    )
                }

                this.edges << subE
            }

            int i = -1;
            for (def x: dependencies) {
                i += 1
                if (x == null)
                    continue

                def subV = inputs[i]
                edges.add(new Edge(x.key, x.value, subV))
            }

            v = new OutputVertex(
                "${expr.methodAsString} Output",
                entity.outputs
            )

            addVertex(v)

            for (def output: entity.outputs) {

                def prod = idMapping[subWorkflowDag.scope.get(output)]

                edges.add(new Edge(output, prod, v))
            }

            // todo(ayush): enable this always to support tuple unpacking
            if (entity.outputs.size() == 0) {
                scope.set("${expr.methodAsString}.out", v)
            }

            for (String o: entity.outputs) {
                scope.set("${expr.methodAsString}.out.$o", v)
            }

            return v
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

            // this.operator(channel, ...args) -> channel.operator(...args)
            def args = newArgs
            if (expr.objectExpression.text == "this") {
                List<Expression> res = []
                newArgs.eachWithIndex {it, idx ->
                    if (idx == 0) return;
                    res << it
                }

                args = res
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
                            new ArgumentListExpression(args),
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
                        new MethodCallExpression(
                            expr.objectExpression,
                            expr.methodAsString,
                            new ArgumentListExpression(newArgs)
                        )
                    )
                ),
                [new ExpressionStatement(new VariableExpression("res"))]
            )
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

        return scope.get(expr.name)
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
        if (isLiteralExpression(expr)) {
            return null
        }

        def res = scope.get(expr.text)
        if (res != null) return res

        return visitExpression(expr.objectExpression)
    }

    Vertex visitListExpression(ListExpression expr) {
        if (isLiteralExpression(expr)) {
            return null
        }

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
        if (isLiteralExpression(expr)) {
            return null
        }

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
        if (isLiteralExpression(expr)) {
            return null
        }

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


    boolean isLiteralExpression(Expression expr) {
        switch (expr) {
            case BinaryExpression:
                return false
            case MethodCallExpression:
                return false
            case ClosureExpression:
                return false

            case VariableExpression:
                return expr.name == "params"
            case ConstantExpression:
                return true

            case StaticMethodCallExpression:
                return isLiteralExpression(expr.arguments)
            case ArgumentListExpression:
                return !expr.expressions.collect({ !isLiteralExpression(it) }).any()
            case PropertyExpression:
                return isLiteralExpression(expr.objectExpression)
            case BooleanExpression:
                return isLiteralExpression(expr.expression)
            case ListExpression:
                return !expr.expressions.collect({ !isLiteralExpression(it) }).any()
            case MapExpression:
                return !expr.mapEntryExpressions.collect({ !isLiteralExpression(it) }).any()
            case MapEntryExpression:
                return isLiteralExpression(expr.keyExpression) && isLiteralExpression(expr.valueExpression)
            case GStringExpression:
                return !expr.values.collect({ !isLiteralExpression(it) }).any()
            default:
                throw new DAGGenerationException("Cannot process expression of type ${expr?.class}")
        }
    }

    Vertex visitExpressionWithLiteralCheck(Expression expr) {
        // should only be called from expressions that don't generate their own vertices, e.g. set, =
        if (isLiteralExpression(expr)) {
            log.debug "$expr.class: $expr.text --> literal"

            def v = new Vertex(
                Vertex.Type.Literal,
                expr.text,
                new ExpressionStatement(
                    new BinaryExpression(
                        new VariableExpression("res"),
                        Token.newSymbol("=", -1, -1),
                        expr
                    )
                ),
                [new ExpressionStatement(new VariableExpression("res"))]
            )

            addVertex(v)
            return v
        }

        return visitExpression(expr)
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
                            new VariableExpression("condition"),
                            Token.newSymbol("=", 0, 0),
                            sub,
                        )
                    ),
                    [new ExpressionStatement(new VariableExpression("condition"))]
                )
                addVertex(cond)

                if (condProducer != null) {
                    def e = new Edge(condProducer.label, condProducer, cond);
                    edges.add(e)
                }

                def parentScope = this.scope
                def ifScope = new Scope(parentScope)
                def elseScope = new Scope(parentScope)

                this.scope = ifScope
                def ifBlock = stmt.ifBlock as BlockStatement
                this.activeConditionals[cond] = true
                for (def x: ifBlock.statements)
                    visitStatement(x)

                this.scope = elseScope
                // guard against ifs without else blocks
                if (stmt.elseBlock instanceof BlockStatement) {
                    def elseBlock = stmt.elseBlock as BlockStatement
                    this.activeConditionals[cond] = false
                    for (def x: elseBlock.statements)
                        visitStatement(x)
                }

                this.activeConditionals.remove(cond)

                this.scope = parentScope
                for (def name: ifScope.bindings.keySet() + elseScope.bindings.keySet()) {
                    def ifBinding = ifScope.get(name)
                    def elseBinding = elseScope.get(name)

                    if (ifBinding == null || elseBinding == null) {
                        // todo(ayush): we should probably allow this, but we need to make sure that if this explodes
                        //  at runtime, the error is clear (which will be very hard to do right now)
                        throw new DAGGenerationException("Name ${name} is potentially unbound.")
                    }

                    def v = ifBinding

                    // merge separate assignments
                    if (ifBinding.id != elseBinding.id) {
                        v = new MergeVertex("Merge $name")

                        edges << new Edge(name, ifBinding, v)
                        edges << new Edge(name, elseBinding, v)

                        addVertex(v)
                    }

                    this.scope.set(name, v)
                }
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
                scope.set(expr.name, v)
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
