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
                log.error """\
                Attempted to include module before it was parsed 
                    keys: ${scopes.keySet()}
                    looking for: ${currentImportPath}
                """.stripIndent()
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
                // keep track of input ordering here too

                context = stmt.statementLabel ?: context
                if (context != "output")
                    continue

                if (!(stmt instanceof ExpressionStatement) || !((stmt as ExpressionStatement).expression instanceof MethodCallExpression))
                    continue

                MethodCallExpression call = (stmt as ExpressionStatement).expression as MethodCallExpression

                List<Expression> args = isTupleX(call.arguments)?.expressions
                if (args == null) continue

                if (args.size() < 2 && (args.size() != 1 || call.methodAsString != "stdout")) continue

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
                    def fileName = c.text
                    if (!fileName.endsWith(".nf")) {
                        fileName = "${fileName}.nf".toString()
                    }

                    currentImportPath = Path.of(parent.toString(), fileName).normalize().toString()
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

    Set<String> workflowNames = new HashSet<String>()
    int anonymousWorkflows = 0
    Map<String, DAGBuilder> dags = new LinkedHashMap<String, DAGBuilder>()

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
                    output.add(x)
                    break
                case 'main':
                default:
                    body.add(x)
            }
        }

        def dagBuilder = new DAGBuilder(name, currentScope, module, this)

        for (def s: input) dagBuilder.addInputNode(s)
        for (def s: body) dagBuilder.visit(s)
        for (def s: output) dagBuilder.visit(s)

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

        workflowNames.forEach {name -> this.dags[name].writeDAG()}
    }
}
