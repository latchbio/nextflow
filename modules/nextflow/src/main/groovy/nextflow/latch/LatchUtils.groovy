package nextflow.latch

import java.nio.file.Path
import java.nio.file.Paths
import java.util.regex.Matcher

import groovy.json.JsonBuilder
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import java.util.Base64

import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.DataflowBroadcast
import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowReadChannel
import groovyx.gpars.dataflow.DataflowVariable
import groovyx.gpars.dataflow.expression.DataflowExpression
import nextflow.Channel
import nextflow.Nextflow
import nextflow.extension.CH
import nextflow.file.FileHelper
import nextflow.file.http.XPath
import nextflow.script.TokenBranchChoice
import nextflow.script.TokenMultiMapDef

@Slf4j
class LatchUtils {

    private static decoder = Base64.getDecoder()

    static Object deserialize0(Object json) {
        if (!(json instanceof Map))
            throw new Exception("Cannot deserialize malformed JSON ${json}")

        if (json.containsKey("null")) {
            return null;
        }

        for (def k: ["boolean", "string", "integer", "float", "double", "number"]) {
            if (!json.containsKey(k)) continue;
            return json.get(k)
        }

        if (json.containsKey("path")) {
            String uriString = json.get("path")
            return FileHelper.asPath(uriString)
        } else if (json.containsKey("value")) {
            def result = new DataflowVariable()
            result << deserialize0(json.get("value"))
            return result
        } else if (json.containsKey("branchChoice")) {
            def tbc = json.get("branchChoice") as Map
            return new TokenBranchChoice(
                deserialize0(tbc.get("value")),
                tbc.get("choice") as String
            )
        } else if (json.containsKey("list")) {
            def lst = json.get("list") as List
            return lst.collect { deserialize0(it) }
        } else if (json.containsKey("map")) {
            def lst = json.get("map") as List
            def res = [:]

            lst.collect {
                if (!(it instanceof Map) || !it.containsKey("key") || !it.containsKey("value"))
                    throw new Exception("Cannot deserialize malformed JSON Map Item ${it}")

                def k = deserialize0(it.get("key"))
                def v = deserialize0(it.get("value"))

                res[k] = v;
            }

            return res
        } else if (json.containsKey("object")) {
            String encoded = json.get("object")

            def byteData = decoder.decode( encoded );
            def ois = new ObjectInputStream( new ByteArrayInputStream(  byteData ) );

            def deserializedVal = ois.readObject();
            ois.close();

            return deserializedVal
        }

        throw new Exception("Cannot deserialize unsupported JSON Item ${json}")
    }

    static List deserializeParams(String serializedJson) {
        def slurper = new JsonSlurper()
        List channels = slurper.parseText(serializedJson) as List

        def params = []
        for (Object channel: channels) {
            if (channel instanceof Map) {
                def val = deserialize0(channel)

                if (!(val instanceof DataflowExpression))
                    throw new Exception("non-list channel input must be a value channel: $channel")

                params << val
                continue
            }

            if (!(channel instanceof List<String>))
                throw new Exception("non-list channel input must be a value channel: $channel")

            def res = []
            for (def x: channel) {
                res << deserialize0(x)
            }
            params << res
        }

        return params
    }

    static Map serialize(Object value) {
        if (value instanceof DataflowExpression) {
            return ["value": serialize(value.getVal())]
        } else if (value instanceof DataflowBroadcast || value instanceof DataflowQueue) {
            value.bind(Channel.STOP)

            return serialize(CH.getReadChannel(value))
        }

        if (value == null) {
            return ["null": null]
        } else if (value instanceof Boolean) {
            return ["boolean": value]
        } else if (value instanceof String) {
            return ["string": value]
        } else if (value instanceof GString) {
            return ["string": value.toString()]
        } else if (value instanceof Integer) {
            return ["integer": value]
        } else if (value instanceof Float) {
            return ["float": value]
        } else if (value instanceof Double) {
            return ["double": value]
        } else if (value instanceof Number) {
            return ["number": value]
        } else if (value instanceof Path) {
            return ["path": value.toUriString()]
        } else if (value instanceof List) {
            List<Object> res = value.collect { serialize(it) }

            return ["list": res]
        } else if (value instanceof Map) {
            List<Map<String, Object>> res = []
            value.forEach {k, v ->
                res << (["key": serialize(k), "value": serialize(v)] as Map<String, Object>)
            }

            return ["map": res]
        } else if (value instanceof TokenBranchChoice) {
            return serialize(value.value)
        } else if (value instanceof Serializable) {
            def baos = new ByteArrayOutputStream();
            def oos = new ObjectOutputStream( baos );
            try {
                oos.writeObject( value );
            } catch (NotSerializableException e) {
                throw new Exception("Unable to serialize value $value of type ${value.getClass()} as it is not Serializable")
            }
            oos.close();

            def encoded = Base64.getEncoder().encodeToString(baos.toByteArray())

            return ["object": encoded]
        } else {
            throw new Exception("Unable to serialize value $value of type ${value.getClass()}")
        }
    }
}

