package nextflow.dagGeneration

import groovy.json.JsonBuilder
import groovy.util.logging.Slf4j
import groovyjarjarantlr4.v4.misc.OrderedHashMap
import nextflow.ast.ASTHelpers
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
import org.codehaus.groovy.ast.expr.NotExpression
import org.codehaus.groovy.ast.expr.PropertyExpression
import org.codehaus.groovy.ast.expr.RangeExpression
import org.codehaus.groovy.ast.expr.StaticMethodCallExpression
import org.codehaus.groovy.ast.expr.TernaryExpression
import org.codehaus.groovy.ast.expr.TupleExpression
import org.codehaus.groovy.ast.expr.VariableExpression
import org.codehaus.groovy.ast.stmt.BlockStatement
import org.codehaus.groovy.ast.stmt.ExpressionStatement
import org.codehaus.groovy.ast.stmt.IfStatement
import org.codehaus.groovy.ast.stmt.Statement
import org.codehaus.groovy.syntax.Token
import org.codehaus.groovy.syntax.Types

@Slf4j
class WorkflowVisitor {
    static class DAGGenerationException extends Exception {
        DAGGenerationException(String msg) {
            super(msg)
        }
    }

    static final Set<String> channelFactories = new HashSet([
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
    static final Set<String> operatorNames = new HashSet([
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

    static final Set<String> specialOperatorNames = new HashSet(["branch", "multiMap"])

    static final Set<String> implicitFunctionNames = new HashSet([
        "branchCriteria",
        "error",
        "exit",
        "file",
        "files",
        "groupKey",
        "multiMapCriteria",
        "sendMail",
        "tuple"
    ])

    String workflowName
    String module = ""
    // process / workflow names
    Map<String, NFEntity> entities
    DAGGeneratorImpl generator
    Scope scope

    DAG<Vertex, Edge> dag;

    WorkflowVisitor(String workflowName, Map<String, NFEntity> entities, String module, DAGGeneratorImpl generator) {
        this.workflowName = workflowName
        this.entities = entities
        this.module = module
        this.generator = generator
        this.scope = new Scope()
        this.dag = new DAG()
    }


    WorkflowVisitor make_clone() {
        Scope newScope = new Scope(scope)
        DAG<Vertex, Edge> newDag = this.dag.make_clone()

        def wv = new WorkflowVisitor(
            workflowName,
            entities,
            module,
            generator
        )
        wv.scope = newScope
        wv.dag = newDag

        return wv
    }

    List<Vertex> inputNodes = new ArrayList<>()

    /*
     * cond in activeConditionals => cond is active
     * activeConditionals[cond] = which branch we are currently processing
     */
    Map<ConditionalVertex, Boolean> activeConditionals = new LinkedHashMap<>()

    ScopeVariable visitBinaryExpression (BinaryExpression expr) {
        // currently supports:
        // - a = expr
        // - (a, b, ..., z) = expr
        // - a comp b for comp in [<, >, <=, >=]
        // - a | b --> b(a())
        // - a op b for op in [+, -, *, /, %, **, ^]

        switch (expr.operation.type) {
            case Types.EQUAL:
                if (expr.leftExpression instanceof VariableExpression) {
                    def dep = visitExpressionWithLiteralCheck(expr.rightExpression)
                    def v = expr.leftExpression as VariableExpression

                    scope.set(v.name, dep)

                    return null
                } else if (expr.leftExpression instanceof TupleExpression) {
                    def t = expr.leftExpression as TupleExpression

                    def rExpList = ASTHelpers.isListX(expr.rightExpression)
                    if (rExpList != null) {
                        if (t.expressions.size() != rExpList.expressions.size()) {
                            throw new DAGGenerationException(
                                "Invalid multiple assignment statement: right and left don't have the same number of elements."
                            )
                        }

                        for (int i = 0; i < t.expressions.size(); i++) {
                            def subLeft = t.expressions[i]
                            def subRight = rExpList.expressions[i]

                            visitBinaryExpression(
                                new BinaryExpression(
                                    subLeft,
                                    Token.newSymbol("=", -1, -1),
                                    subRight,
                                )
                            )
                        }

                        return null
                    }

                    throw new DAGGenerationException("Multiple assignment from non-literal Lists is not supported yet.")

                } else {
                    throw new DAGGenerationException(
                        "Cannot handle left expression in binary expression of type $expr.leftExpression.class: $expr.leftExpression.text"
                    )
                }

                return null
            case Types.PIPE:
                def method = expr.rightExpression
                def arg = expr.leftExpression

                if (arg instanceof VariableExpression && scope.get(arg.text) == null) {
                    // proc_1 | proc_2 => proc_2(proc_1()), not proc_2(proc_1)
                    arg = new MethodCallExpression(
                        new ConstantExpression("this"),
                        arg.text,
                        new ArgumentListExpression([])
                    )
                }

                MethodCallExpression call = new MethodCallExpression(
                    new ConstantExpression("this"),
                    method.text,
                    new ArgumentListExpression([arg])
                )

                if (method instanceof MethodCallExpression) {
                    def args = method.arguments

                    if (args instanceof TupleExpression) {
                        args = new TupleExpression([arg, *args.expressions])
                    } else if (args instanceof ArgumentListExpression) {
                        args = new ArgumentListExpression([arg, *args.expressions])
                    }

                    call = new MethodCallExpression(
                        method.objectExpression,
                        method.method,
                        args
                    )
                }

                return visitMethodCallExpression(call)
            case Types.BITWISE_AND:
                throw new DAGGenerationException("'&' operator is not currently supported")
            default:
                def left = visitExpression(expr.leftExpression)
                if (expr.operation.type == Types.LEFT_SQUARE_BRACKET) {
                    def right = ASTHelpers.isConstX(expr.rightExpression)
                    if (
                        (left instanceof ProcessVariable || left instanceof SubWorkflowVariable)
                            && right != null
                    ) {
                        def idx = right.value

                        if (idx instanceof Integer) {
                            // .properties is an ordered hashmap so this access is fine
                            return left.properties.values()[idx]
                        }
                    }
                }

                def right = visitExpression(expr.rightExpression)
                Expression call
                if ((left == null) && (right == null)) {
                    call = expr
                } else if (left == null) {
                    call = new MethodCallExpression(
                        new MethodCallExpression(
                            new VariableExpression("Channel"),
                            "placeholder",
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
                            "placeholder",
                            new ArgumentListExpression([])
                        )

                    call = new MethodCallExpression(
                        new MethodCallExpression(
                            new VariableExpression("Channel"),
                            "placeholder",
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
                    addDependency(left, v)
                if (right != null)
                    addDependency(right, v)

                return new ScopeVariable(v)
        }
    }

    ScopeVariable visitTernaryExpression (TernaryExpression expr) {
        def bool = visitExpression(expr.booleanExpression)

        def sub = expr.booleanExpression
        if (bool != null) {
            sub = new MethodCallExpression(
                new VariableExpression("Channel"),
                "placeholder",
                new ArgumentListExpression([])
            )
        }

        def cond = new ConditionalVertex(
            expr.booleanExpression.text,
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
        if (bool != null) {
            addDependency(bool, cond)
        }

        this.activeConditionals[cond] = true
        def left = visitExpressionWithLiteralCheck(expr.trueExpression)

        this.activeConditionals[cond] = false
        def right = visitExpressionWithLiteralCheck(expr.falseExpression)

        this.activeConditionals.remove(cond)

        def v = new MergeVertex(expr.text)
        addVertex(v)

        // todo(ayush): this breaks with processes
        addDependency(left, v)
        addDependency(right, v)

        return new ScopeVariable(v)
    }

    ScopeVariable visitMethodCallExpression (MethodCallExpression expr) {
        if (expr.methodAsString == "set") {
            def lst = (ArgumentListExpression) expr.arguments

            if (lst.size() != 1)
                throw new DAGGenerationException("Channel.set() takes exactly one argument, provided ${lst.size()} arguments: $lst")

            ClosureExpression ce = ASTHelpers.isClosureX(lst[0])
            if (ce == null) {
                throw new DAGGenerationException("Channel.set() argument must be a closure, provided ${lst[0].class}: ${lst[0]}")
            }

            BlockStatement b = ASTHelpers.isBlockStmt(ce.code)
            if (b == null) {
                throw new DAGGenerationException("Malformed closure: ${lst[0]}")
            }

            if (b.statements.size() != 1)
                throw new DAGGenerationException("Cannot parse closure argument to Channel.set() w/ more than one statement: $b")

            ExpressionStatement stmt = b.statements[0] as ExpressionStatement
            VariableExpression ve = ASTHelpers.isVariableX(stmt.expression)
            if (ve == null)
                throw new DAGGenerationException("Statement in closure argument to Channel.set() must be a variable: $b")

            return visitBinaryExpression(new BinaryExpression(ve, Token.newSymbol("=", -1, -1), expr.objectExpression))
        } else if (
            (expr.objectExpression.text == "Channel" && expr.methodAsString in channelFactories)
            || (expr.objectExpression.text == "this" && expr.methodAsString in implicitFunctionNames)
        ) {
            // assume all calls of this form have literal arguments bc passing a Channel to them is an error in vanilla nextflow
            Vertex v = new Vertex(
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

            addVertex(v)
            ScopeVariable outVar = new ScopeVariable(v)
            return outVar
        }

        Map<Integer, ScopeVariable> dependencies = new OrderedHashMap<Integer, ScopeVariable>()
        int depIdx = 0
        def objProducer = visitExpression(expr.objectExpression)
        if (objProducer != null)
            dependencies[depIdx] = objProducer

        def entity = this.entities.get(expr.methodAsString)

        List<Expression> newArgs = []
        if (expr.arguments instanceof TupleExpression) {
            for (def x: (TupleExpression) expr.arguments) {
                depIdx += 1

                def me = ASTHelpers.isMapEntryX(x)
                if (me != null) {
                    // hack: this assumes that all keyword arguments are literals,
                    // which seems to be the case in the examples ive seen but could be wrong
                    newArgs.add(x)
                    continue
                }

                ScopeVariable producer
                if (entity?.type == NFEntity.Type.Workflow) {
                    producer = visitExpressionWithLiteralCheck(x)
                } else {
                    producer = visitExpression(x)
                }

                if (producer == null) {
                    newArgs.add(x)
                    continue
                }

                if (producer instanceof ProcessVariable) {
                    producer.properties.collect {
                        newArgs << new MethodCallExpression(
                            new VariableExpression('Channel'),
                            "placeholder",
                            new ArgumentListExpression([])
                        )
                    }
                } else {
                    newArgs.add(
                        new MethodCallExpression(
                            new VariableExpression('Channel'),
                            "placeholder",
                            new ArgumentListExpression([])
                        )
                    )
                }

                dependencies[depIdx] = producer
            }
        }

        if (entity?.type == NFEntity.Type.Process) {
            List<Statement> ret
            if (entity.outputs.size() > 0) {
                def outExpr = new PropertyExpression(
                    new VariableExpression(expr.methodAsString),
                    "out"
                )

                int idx = 0
                ret = entity.outputs.collect {
                    new ExpressionStatement(
                        new BinaryExpression(
                            new VariableExpression(it),
                            Token.newSymbol("=", 0, 0),
                            new BinaryExpression(
                                outExpr,
                                Token.newSymbol("[", -1, -1),
                                new ConstantExpression(idx++)
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

            Vertex v = new ProcessVertex(
                expr.methodAsString,
                new ExpressionStatement(
                    new MethodCallExpression(
                        new VariableExpression('this'),
                        new ConstantExpression(expr.methodAsString),
                        new ArgumentListExpression(newArgs as List<Expression>)
                    )
                ),
                ret,
                entity.outputs,
                entity.module,
                entity.unaliased
            )

            v.cpus = entity.cpus
            v.memoryBytes = entity.memoryBytes

            addVertex(v)
            for (def x: dependencies) {
                addDependency(x.value, v)
            }

            OrderedHashMap<String, PropertyVariable> props = new OrderedHashMap<String, PropertyVariable>()
            int idx = 0
            for (String name: entity.outputs) {
                props[name] = new PropertyVariable(v, idx++)
            }

            def processVariable = new ProcessVariable(v, props)
            scope.set(expr.methodAsString, processVariable)
            return processVariable
        } else if (entity?.type == NFEntity.Type.Workflow) {
            List<Vertex> inputs = []
            WorkflowVisitor subWorkflowDag = generator.visitors[entity.unaliased]
            Map<Vertex, Vertex> idMapping = new HashMap<Vertex, Vertex>();

            for (def _subV: subWorkflowDag.dag.vertices) {
                // maintain distinct IDs (only a problem if a subworkflow is called more than once)
                def subV = _subV.make_clone()
                idMapping[_subV] = subV

                if (subV.type == Vertex.Type.Input) {
                    inputs << subV
                } else {
                    addVertex(subV)
                }
            }

            Map<Vertex, ScopeVariable> inputBindings = new HashMap<Vertex, ScopeVariable>()

            int i = 0;
            for (def x: dependencies) {
                ScopeVariable dep = x.value

                if (dep instanceof ProcessVariable) {
                    // tuple unpacking
                    for (PropertyVariable prop: dep.properties.values()) {
                        inputBindings[inputs[i++]] = prop
                    }
                } else if (dep instanceof SubWorkflowVariable) {
                    // tuple unpacking
                    for (ScopeVariable prop: dep.properties.values()) {
                        inputBindings[inputs[i++]] = prop
                    }
                } else {
                    inputBindings[inputs[i++]] = dep
                }
            }

            def inputSet = new HashSet<Vertex>(inputs);

            for (def _subE: subWorkflowDag.dag.edges) {
                def from = idMapping[_subE.from]
                def to = idMapping[_subE.to]

                if (from in inputSet) {
                    addDependency(inputBindings[from], to)
                    continue
                }

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

                this.dag.addEdge(subE)
            }

            OrderedHashMap<String, ScopeVariable> properties = new OrderedHashMap<String, ScopeVariable>();

            for (def outputName: entity.outputs) {
                def outVar = subWorkflowDag.scope.get(outputName)
                properties[outputName] = outVar.remap(idMapping)
            }

            scope.set(expr.methodAsString, new SubWorkflowVariable(properties))

            return null
        } else if (expr.methodAsString in operatorNames) {

            def ret = [new ExpressionStatement(new VariableExpression("res"))]

            List<String> outputNames = [];
            if (expr.methodAsString in specialOperatorNames) {
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

            Vertex v = new Vertex(
                Vertex.Type.Operator,
                expr.methodAsString,
                new ExpressionStatement(
                    new BinaryExpression(
                        new VariableExpression("res"),
                        Token.newSymbol("=", 0, 0),
                        new MethodCallExpression(
                            new MethodCallExpression(
                                new VariableExpression('Channel'),
                                "placeholder",
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

            addVertex(v)
            for (def dep: dependencies) {
                addDependency(dep.value, v)
            }

            ScopeVariable outVar = new ScopeVariable(v)
            if (expr.methodAsString in specialOperatorNames) {
                OrderedHashMap<String, PropertyVariable> props = new OrderedHashMap<String, PropertyVariable>()
                int i = 0
                for (def outputName: outputNames) {
                    props[outputName] = new PropertyVariable(v, i++)
                }

                outVar = new SpecialOperatorVariable(v, props)
            }

            return outVar
        } else {
            Vertex v = new Vertex(
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

            addVertex(v)
            for (def dep: dependencies) {
                addDependency(dep.value, v)
            }

            ScopeVariable outVar = new ScopeVariable(v)
            return outVar
        }
    }

    ScopeVariable visitVariableExpression(VariableExpression expr) {
        // visiting a variable expression var should look up
        // and return the most recent producer of var

        return scope.get(expr.name)
    }

    ScopeVariable visitClosureExpression(ClosureExpression expr) {

        // for the most part we don't need to care about closures, except for when the parent expr is a MCE and the method is
        // - branch: we need to figure out what the forks are called
        // - multiMap: same thing
        // - set: update the producer of the returned variable name to be the producer of the left expression
        // ^ these are handled in the MCE logic directly

        // todo(ayush): what are the cases where a closure has a nontrivial producer?
        //  - can you reference other channels within closures?

        return null
    }

    ScopeVariable visitPropertyExpression(PropertyExpression expr) {
        if (isLiteralExpression(expr)) {
            return null
        }

        def obj = visitExpression(expr.objectExpression)

        if (obj instanceof ProcessVariable || obj instanceof SubWorkflowVariable) {
            Map<String, ScopeVariable> props = obj.properties as Map<String, ScopeVariable>

            if (expr.propertyAsString != "out") {
                return props.get(expr.propertyAsString)
            }

            if (props.size() == 1) {
                // process.out -> channel
                for (def prop: props) {
                    return prop.value
                }
            }

            // process.out -> map of channels
            return obj
        } else if (obj instanceof SpecialOperatorVariable) {
            return obj.properties.get(expr.propertyAsString)
        }

        return obj
    }

    ScopeVariable visitListExpression(ListExpression expr) {
        if (isLiteralExpression(expr)) {
            return null
        }

        List<ScopeVariable> dependencies = []

        List<Expression> transformed = expr.expressions.collect {
            def dep = visitExpression(it)
            if (dep == null) {
                return it
            }

            dependencies << dep

            return new MethodCallExpression(
                new VariableExpression("Channel"),
                "placeholder",
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
        dependencies.each {
            addDependency(it, v)
        }

        return new ScopeVariable(v)
    }


    ScopeVariable visitMapExpression(MapExpression expr) {
        if (isLiteralExpression(expr)) {
            return null
        }

        List<ScopeVariable> dependencies = []
        List<MapEntryExpression> transformed = expr.mapEntryExpressions.collect {
            def keyDep = visitExpression(it.keyExpression)
            Expression keyExpr = it.keyExpression
            if (keyDep != null) {
                dependencies << keyDep
                keyExpr = new MethodCallExpression(
                    new VariableExpression("Channel"),
                    "placeholder",
                    new ArgumentListExpression([])
                )
            }

            def valDep = visitExpression(it.valueExpression)
            Expression valExpr = it.valueExpression
            if (valDep != null) {
                dependencies << valDep
                valExpr = new MethodCallExpression(
                    new VariableExpression("Channel"),
                    "placeholder",
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
        dependencies.each {
            addDependency(it, v)
        }

        return new ScopeVariable(v)
    }


    ScopeVariable visitGStringExpression(GStringExpression expr) {
        if (isLiteralExpression(expr)) {
            return null
        }

        List<ScopeVariable> dependencies = []
        List<Expression> transformed = expr.values.collect {
            def dep = visitExpression(it)
            if (dep == null) {
                return it
            }

            dependencies << dep

            return new MethodCallExpression(
                new VariableExpression("Channel"),
                "placeholder",
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
        dependencies.each {
            addDependency(it, v)
        }

        return new ScopeVariable(v)
    }

    ScopeVariable visitBooleanExpression(BooleanExpression expr) {
        def resExpr = new NotExpression(new NotExpression(expr.expression))

        def dep = visitExpression(expr.expression)
        if (dep != null) {
            resExpr = new MethodCallExpression(
                new MethodCallExpression(
                    new VariableExpression("Channel"),
                    "placeholder",
                    new ArgumentListExpression([])
                ),
                "toBoolean",
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
                    resExpr
                )
            ),
            [new ExpressionStatement(new VariableExpression("res"))]
        )

        addVertex(v)
        if (dep != null) {
            addDependency(dep, v)
        }

        return new ScopeVariable(v)
    }

    boolean isLiteralExpression(Expression expr) {
        switch (expr) {
            case MethodCallExpression:
                return false
            case ClosureExpression:
                return false

            case VariableExpression:
                return expr.name == "params"
            case ConstantExpression:
                return true

            case BinaryExpression:
                return isLiteralExpression(expr.leftExpression) && isLiteralExpression(expr.rightExpression)
            case TernaryExpression:
                return (
                    isLiteralExpression(expr.booleanExpression)
                    && isLiteralExpression(expr.trueExpression)
                    && isLiteralExpression(expr.falseExpression)
                )
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

    ScopeVariable visitRangeExpression(RangeExpression expr) {
        // assume ranges are literal expressions for now
        return null
    }

    ScopeVariable visitExpressionWithLiteralCheck(Expression expr) {
        // should only be called from expressions that don't generate their own vertices, e.g. set, =
        if (isLiteralExpression(expr)) {
            def v = new Vertex(
                Vertex.Type.Generator,
                expr.text,
                new ExpressionStatement(
                    new BinaryExpression(
                        new VariableExpression("res"),
                        Token.newSymbol("=", -1, -1),
                        new MethodCallExpression(
                            new VariableExpression("Channel"),
                            "value",
                            new ArgumentListExpression([expr])
                        )
                    )
                ),
                [new ExpressionStatement(new VariableExpression("res"))]
            )

            addVertex(v)
            return new ScopeVariable(v)
        }

        return visitExpression(expr)
    }

    ScopeVariable visitExpression(Expression expr) {
        ScopeVariable res
        switch (expr) {
            case BinaryExpression:
                res = visitBinaryExpression(expr)
                break
            case TernaryExpression:
                res = visitTernaryExpression(expr)
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
                res = visitBooleanExpression(expr)
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
            case RangeExpression:
                res = visitRangeExpression(expr)
                break
            default:
                throw new DAGGenerationException("Cannot process expression of type ${expr?.class}")
        }

        return res
    }

    void addVertex(Vertex v) {
        if (v.subWorkflowName == null) {
            v.subWorkflowName = workflowName
            v.subWorkflowPath = module
        }

        this.dag.addVertex(v)

        this.activeConditionals.forEach {cond, branch ->
            this.dag.addEdge(new ConditionalEdge(cond.call.text, cond, v, branch))
        }
    }

    void addDependency(ScopeVariable src, Vertex dst) {
        if (src == null || dst == null) return
        switch (src) {
            case PropertyVariable:
                addEdge(src.index.toString(), src.vertex, dst)
                break
            case ProcessVariable:
                src.properties.collect {_, prop ->
                    addDependency(prop, dst)
                }
                break
            case SubWorkflowVariable:
                src.properties.collect {_, prop ->
                    addDependency(prop, dst)
                }
                break
            default:
                addEdge("", src.vertex, dst)
        }
    }

    void addEdge(String label, Vertex src, Vertex dst) {
        if (src == null || dst == null) return
        this.dag.addEdge(new Edge(label, src, dst))
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
                        "placeholder",
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
                    addDependency(condProducer, cond)
                }

                def parentScope = this.scope
                def ifScope = new Scope(parentScope)
                def elseScope = new Scope(parentScope)

                this.scope = ifScope

                def ifBlock = ASTHelpers.isBlockStmt(stmt.ifBlock)
                if (ifBlock == null) {
                    ifBlock = new BlockStatement([stmt.ifBlock], null)
                }

                this.activeConditionals[cond] = true
                for (def x: ifBlock.statements) {
                    visitStatement(x)
                }

                this.scope = elseScope
                // guard against ifs without else blocks
                if (stmt.elseBlock != null) {
                    def elseBlock = ASTHelpers.isBlockStmt(stmt.elseBlock)
                    if (elseBlock == null) {
                        elseBlock = new BlockStatement([stmt.elseBlock], null)
                    }
                    this.activeConditionals[cond] = false
                    for (def x: elseBlock.statements) {
                        visitStatement(x)
                    }
                }

                this.activeConditionals.remove(cond)

                this.scope = parentScope
                outer:
                for (def name: ifScope.bindings.keySet() + elseScope.bindings.keySet()) {
                    def ifBinding = ifScope.get(name)
                    def elseBinding = elseScope.get(name)
                    
                    ScopeVariable binding = ifBinding

                    if (ifBinding == null || elseBinding == null) {
                        // todo(ayush): there is potential here for using unbound variables downstream - we need to make
                        //  sure that if this explodes at runtime, the error is clear
                        binding = ifBinding ?: elseBinding
                    } else if (ifBinding != elseBinding) {
                        def v = new MergeVertex("Merge $name")

                        if (ifBinding instanceof ProcessVariable || ifBinding instanceof SubWorkflowVariable) {
                            if (ifBinding.class != elseBinding.class) {
                                log.warn "Warning: type error when merging ${name} - Variables of the same name must have the same type."
                                continue
                            }

                            for (def entry: ifBinding.properties) {
                                if (entry.key in elseBinding.properties)
                                    continue

                                log.warn "Warning: type error when merging ${name} - Variables of the same name must have the same type."
                                continue outer
                            }
                            for (def entry: elseBinding.properties) {
                                if (entry.key in ifBinding.properties)
                                    continue

                                log.warn "Warning: type error when merging ${name} - Variables of the same name must have the same type."
                                continue outer
                            }

                            v.outputNames = ifBinding.properties.keySet().toList()
                        }

                        addVertex(v)
                        addDependency(ifBinding, v)
                        addDependency(elseBinding, v)

                        binding = ifBinding.makeClone()

                        if (!(binding instanceof SubWorkflowVariable)) {
                            binding.vertex = v
                        }

                        if (binding instanceof ProcessVariable || binding instanceof SubWorkflowVariable) {
                            for (def prop: binding.properties.values()) {
                                (prop as ScopeVariable).vertex = v
                            }
                        }
                    }

                    this.scope.set(name, binding)
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
            
            ${module} - ${workflowName} - L${stmt.lineNumber}
            $stmt.text

            Error:
            $e
            """.stripIndent()

            System.exit(1)
        }
    }

    // todo(ayush): get rid of input nodes entirely
    void addInputNode(Statement stmt) {
        if (stmt instanceof ExpressionStatement) {
            def expr = stmt.expression
            if (expr instanceof VariableExpression) {
                def v = new Vertex(Vertex.Type.Input, expr.name, null, null)

                addVertex(v)
                inputNodes.add(v)
                scope.set(expr.name, new ScopeVariable(v))
            }
        }
    }


    void writeDAG() {
        def vbs = new ArrayList<JsonBuilder>()
        def ebs = new ArrayList<JsonBuilder>()

        for (Vertex v: this.dag.vertices) {
            def vb = new JsonBuilder([
                id: v.id.toString(),
                label: v.label,
                type: v.type,
                statement: v.call != null ? StatementJSONConverter.toJsonString(v.call) : null,
                ret: v.ret != null ? v.ret.collect {StatementJSONConverter.toJsonString(it)} : null,
                outputNames: v.outputNames,
                module: v.module,
                unaliased: v.unaliased,
                subWorkflowName: v.subWorkflowName,
                subWorkflowPath: v.subWorkflowPath,
                cpu: v.cpus,
                memoryBytes: v.memoryBytes,
            ])

            vbs.add(vb)
        }

        for (Edge e: this.dag.edges) {
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
