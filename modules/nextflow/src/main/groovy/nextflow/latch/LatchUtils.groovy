package nextflow.latch

import java.nio.file.Path
import java.nio.file.Paths

import groovy.json.JsonBuilder
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import java.util.Base64

import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.DataflowBroadcast
import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowReadChannel
import groovyx.gpars.dataflow.DataflowVariable
import nextflow.Channel
import nextflow.extension.CH
import nextflow.file.http.XPath

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
            String path = json.get("path")
            return Paths.get(path)
        } else if (json.containsKey("list")) {
            def lst = json.get("list") as List
            return lst.collect { deserialize0(it) }
        } else if (json.containsKey("map")) {
            def lst = json.get("list") as List
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

    static Object deserialize(String val) {
        def slurper = new JsonSlurper()

        Object parsed = slurper.parseText(val)
        return deserialize0(parsed)
    }

    static List deserializeParams(String serializedJson) {
        def slurper = new JsonSlurper()
        List<List<String>> channels = slurper.parseText(serializedJson)

        def params = []
        for (Object channel: channels) {
            def res = []
            for (def x: channel) {
                res << deserialize0(x)
            }
            params << res
        }

        return params
    }

    static Map serialize(Object value) {
        if (value instanceof DataflowVariable) {
            return serialize(value.get())
        } else if (value instanceof DataflowReadChannel) {
            return serialize(value.getVal())
        } else if (value instanceof DataflowBroadcast || value instanceof DataflowQueue) {
            value.bind(Channel.STOP)

            return serialize(CH.getReadChannel(value))
        }

        if (value == null) {
            return ["null": null]
        } else if (value instanceof Boolean) {
            return  ["boolean": value]
        } else if (value instanceof String) {
            return  ["string": value]
        } else if (value instanceof Integer) {
            return  ["integer": value]
        } else if (value instanceof Float) {
            return  ["float": value]
        } else if (value instanceof Double) {
            return  ["double": value]
        } else if (value instanceof Number) {
            return  ["number": value]
        } else if (value instanceof Path) {
            return  ["path": value.toString()]
        } else if (value instanceof List) {
            List<Object> res = value.collect { serialize(it) }

            return ["list": res]
        } else if (value instanceof Map) {
            List<Map<String, Object>> res = []
            value.forEach {k, v ->
                res << (["key": serialize(k), "value": serialize(v)] as Map<String, Object>)
            }

            return ["map": res]
        } else if (value instanceof Serializable) {
            return serializeObject(value)
        } else {
            throw new Exception("Unable to serialized value $value of type ${value.getClass()}")
        }
    }

    static Map<String, String> serializeObject(Serializable value) {
        def baos = new ByteArrayOutputStream();
        def oos = new ObjectOutputStream( baos );
        oos.writeObject( value );
        oos.close();

        def encoded = Base64.getEncoder().encodeToString(baos.toByteArray())

        return ["object": encoded]
    }

    static String serializeParam(Object value) {
        def builder = new JsonBuilder()
        builder serialize(value)

        return builder.toString()
    }
}

