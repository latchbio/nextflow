package nextflow.dagGeneration

import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j
import nextflow.Nextflow
import org.codehaus.groovy.ast.ClassCodeVisitorSupport
import org.codehaus.groovy.ast.ClassHelper
import org.codehaus.groovy.ast.ClassNode
import org.codehaus.groovy.ast.Parameter
import org.codehaus.groovy.ast.VariableScope
import org.codehaus.groovy.ast.expr.ArgumentListExpression
import org.codehaus.groovy.ast.expr.BinaryExpression
import org.codehaus.groovy.ast.expr.BooleanExpression
import org.codehaus.groovy.ast.expr.ClassExpression
import org.codehaus.groovy.ast.expr.ClosureExpression
import org.codehaus.groovy.ast.expr.ConstantExpression
import org.codehaus.groovy.ast.expr.ConstructorCallExpression
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
import org.codehaus.groovy.ast.stmt.EmptyStatement
import org.codehaus.groovy.ast.stmt.ExpressionStatement
import org.codehaus.groovy.ast.stmt.IfStatement
import org.codehaus.groovy.ast.stmt.ReturnStatement
import org.codehaus.groovy.ast.stmt.Statement
import org.codehaus.groovy.control.SourceUnit
import org.codehaus.groovy.syntax.Token

@Slf4j
class StatementJSONConverter {
    static class JsonConverter extends ClassCodeVisitorSupport {
        JsonConverter() {}

        SourceUnit getSourceUnit() {return null}

        Map visitExpression(Expression expr) {
            switch(expr) {
                case ConstantExpression:
                    return ["ConstantExpression": expr.value]
                case VariableExpression:
                    return ["VariableExpression": expr.name]
                case NotExpression:
                    return ["NotExpression": visitExpression(expr.expression)]
                case BooleanExpression:
                    return ["BooleanExpression": visitExpression(expr.expression)]
                case ListExpression:
                    return ["ListExpression": expr.expressions.collect {visitExpression(it) }]
                case MapExpression:
                    return ["MapExpression": expr.mapEntryExpressions.collect {visitExpression(it) }]
                case MapEntryExpression:
                    return [
                        "MapEntryExpression": [
                            "keyExpression": visitExpression(expr.keyExpression),
                            "valueExpression": visitExpression(expr.valueExpression)
                        ]
                    ]
                case BinaryExpression:
                    return [
                        "BinaryExpression": [
                            "leftExpression": visitExpression(expr.leftExpression),
                            "operation": expr.operation.text,
                            "rightExpression": visitExpression(expr.rightExpression)
                        ]
                    ]
                case TernaryExpression:
                    return [
                        "TernaryExpression": [
                            "booleanExpression": visitExpression(expr.booleanExpression),
                            "trueExpression": visitExpression(expr.trueExpression),
                            "falseExpression": visitExpression(expr.falseExpression)
                        ]
                    ]
                case MethodCallExpression:
                    return [
                        "MethodCallExpression": [
                            "objectExpression": visitExpression(expr.objectExpression),
                            "method": expr.methodAsString,
                            "arguments": visitExpression(expr.arguments)
                        ]
                    ]
                case StaticMethodCallExpression:
                    return [
                        "StaticMethodCallExpression": [
                            "ownerType": expr.ownerType.text,
                            "method": expr.methodAsString,
                            "arguments": visitExpression(expr.arguments)
                        ]
                    ]
                case ArgumentListExpression:
                    return [
                        "ArgumentListExpression": [
                            "expressions": expr.expressions.collect { visitExpression(it) }
                        ]
                    ]
                case TupleExpression:
                    return [
                        "TupleExpression": [
                            "expressions": expr.expressions.collect { visitExpression(it) }
                        ]
                    ]
                case PropertyExpression:
                    return [
                        "PropertyExpression": [
                            "objectExpression": visitExpression(expr.objectExpression),
                            "property": expr.propertyAsString
                        ]
                    ]
                case ClosureExpression:
                    return [
                        "ClosureExpression": [
                            "code": visit(expr.code),
                            "parameters": expr.parameters.collect { it.name }
                        ]
                    ]
                case GStringExpression:
                    return [
                        "GStringExpression": [
                            "verbatimText": expr.text,
                            "strings": expr.strings.collect { visitExpression(it) },
                            "values": expr.values.collect { visitExpression(it) }
                        ]
                    ]
                case ClassExpression:
                    return [
                        "ClassExpression": [
                            "type": expr.type.text
                        ]
                    ]
                case RangeExpression:
                    return [
                        "RangeExpression": [
                            "from": visitExpression(expr.from),
                            "to": visitExpression(expr.to),
                            "inclusive": expr.inclusive
                        ]
                    ]
                case ConstructorCallExpression:
                    return [
                        "ConstructorCallExpression": [
                            "type": expr.type.text,
                            "arguments": visitExpression(expr.arguments)
                        ]
                    ]
                default:
                    throw new Exception("Cannot JSONify expression of type $expr.class: $expr.text")
            }
        }

        Map visit(Statement s) {
            switch (s) {
                case ExpressionStatement:
                    return [
                        "ExpressionStatement": [
                            "expression": visitExpression(s.expression),
                            "labels": s.statementLabels ?: []
                        ]
                    ]
                case IfStatement:
                    return [
                        "IfStatement": [
                            "booleanExpression": visitExpression(s.booleanExpression),
                            "ifBlock": visit(s.ifBlock),
                            "elseBlock": s.elseBlock != null ? visit(s.elseBlock) : null,
                            "labels": s.statementLabels ?: []
                        ]
                    ]
                case BlockStatement:
                    return [
                        "BlockStatement": [
                            "statements": s.statements.collect { visit(it) },
                            "scope": [
                                "declaredVariables": s.variableScope.declaredVariables.collect {it.key },
                                "referencedClassVariables": s.variableScope.referencedClassVariables.collect {it.key }
                            ],
                            "labels": s.statementLabels ?: []
                        ]
                    ]
                case ReturnStatement:
                    return [
                        "ReturnStatement": visitExpression(s.expression)
                    ]
                case EmptyStatement:
                    return ["EmptyStatement": null]
                default:
                    throw new Exception("Cannot JSONify statement of type ${s?.class}")
            }
        }
    }

    static String toJsonString(Statement s) {
        def converter = new JsonConverter()

        def res
        try {
            res = converter.visit(s)
        } catch (Exception e) {
            throw new Exception("""\
            Ran into exception 
                $e
            When trying to stringify
                ${s.text}
            """.stripIndent())
        }

        def builder = new JsonBuilder()
        builder res
        return builder.toString()
    }

    static class StatementConverter extends ClassCodeVisitorSupport {
        StatementConverter() {}

        SourceUnit getSourceUnit() {return null}

        ClassNode classNodeFromString(String s) {
            switch (s) {
                case "nextflow.Nextflow":
                    return new ClassNode(Nextflow)
                default:
                    throw new Exception("Cannot convert unknown class to ClassNode: $s")
            }
        }

        private static String getKey(Object s) {
            if (!(s instanceof Map)) {
                throw new Exception("Cannot convert non-Map object to statement: $s")
            }

            if (s.keySet().size() != 1) {
                throw new Exception("Cannot convert malformed map w/o exactly 1 key to statement: $s")
            }

            return s.keySet().find({ true })
        }

        Expression visitExpression(Object s) {
            def key = getKey(s)
            def expr = s[key]

            switch (key) {
                case "ConstantExpression":
                    return new ConstantExpression(expr)
                case "VariableExpression":
                    return new VariableExpression(expr as String)
                case "NotExpression":
                    return new NotExpression(visitExpression(expr))
                case "BooleanExpression":
                    return new BooleanExpression(visitExpression(expr))
                case "ListExpression":
                    return new ListExpression(
                        ((List) expr).collect { visitExpression(it) }
                    )
                case "MapExpression":
                    return new MapExpression(
                        ((List) expr).collect { visitExpression(it) as MapEntryExpression }
                    )
                case "MapEntryExpression":
                    return new MapEntryExpression(
                        visitExpression(expr["keyExpression"]),
                        visitExpression(expr["valueExpression"])
                    )
                case "BinaryExpression":
                    return new BinaryExpression(
                        visitExpression(expr["leftExpression"]),
                        Token.newSymbol(expr["operation"] as String, 0, 0),
                        visitExpression(expr["rightExpression"])
                    )
                case "TernaryExpression":
                    return new TernaryExpression(
                        visitExpression(expr["booleanExpression"]) as BooleanExpression,
                        visitExpression(expr["trueExpression"]),
                        visitExpression(expr["falseExpression"])
                    )
                case "MethodCallExpression":
                    return new MethodCallExpression(
                        visitExpression(expr["objectExpression"]),
                        expr["method"] as String,
                        visitExpression(expr["arguments"])
                    )
                case "StaticMethodCallExpression":
                    return new StaticMethodCallExpression(
                        classNodeFromString(expr["ownerType"] as String),
                        expr["method"] as String,
                        visitExpression(expr["arguments"])
                    )
                case "ArgumentListExpression":
                    return new ArgumentListExpression(
                        (expr["expressions"] as List).collect {visitExpression(it)}
                    )
                case "TupleExpression":
                    return new TupleExpression(
                        (expr["expressions"] as List).collect {visitExpression(it)}
                    )
                case "PropertyExpression":
                    return new PropertyExpression(
                        visitExpression(expr["objectExpression"]),
                        expr["property"] as String
                    )
                case "ClosureExpression":
                    return new ClosureExpression(
                        ((expr["parameters"] as List).collect { new Parameter(new ClassNode(Object), it as String) }).toArray() as Parameter[],
                        visit(expr["code"])
                    )
                case "GStringExpression":
                    return new GStringExpression(
                        expr["verbatimText"] as String,
                        (expr["strings"] as List).collect { visitExpression(it) as ConstantExpression },
                        (expr["values"] as List).collect { visitExpression(it) }
                    )
                case "ClassExpression":
                    return new ClassExpression(ClassHelper.make(expr["type"] as String))
                case "RangeExpression":
                    return new RangeExpression(
                        visitExpression(expr["from"]),
                        visitExpression(expr["to"]),
                        expr["inclusive"] as Boolean
                    )
                case "ConstructorCallExpression":
                    return new ConstructorCallExpression(
                        ClassHelper.make(expr["type"] as String),
                        visitExpression(expr["arguments"])
                    )
                default:
                    throw new Exception("Cannot convert malformed JSON into statement: $s")
            }
        }

        Statement visit(Object s) {
            def key = getKey(s)
            def stmt = s[key]

            Statement res
            switch (key) {
                case "ExpressionStatement":
                    res = new ExpressionStatement(
                        visitExpression(stmt["expression"])
                    )
                    break
                case "IfStatement":
                    res = new IfStatement(
                        visitExpression(stmt["booleanExpression"]) as BooleanExpression,
                        visit(stmt["ifBlock"]),
                        visit(stmt["elseBlock"])
                    )
                    break
                case "BlockStatement":
                    res = new BlockStatement(
                        (stmt["statements"] as List).collect { visit(it) },
                        new VariableScope()
                    )
                    break
                case "EmptyStatement":
                    res = new EmptyStatement()
                    break
                case "ReturnStatement":
                    res = new ReturnStatement(visitExpression(stmt))
                    break
                default:
                    throw new Exception("Cannot infer statement from malformed JSON: $s")
            }

            for (def label: stmt["labels"] as List<String>)
                res.addStatementLabel(label)

            return res
        }
    }

    static Statement fromJsonString(String s) {
        def converter = new StatementConverter()
        def slurper = new JsonSlurper()
        def json = slurper.parseText(s)
        return converter.visit(json)
    }
}
