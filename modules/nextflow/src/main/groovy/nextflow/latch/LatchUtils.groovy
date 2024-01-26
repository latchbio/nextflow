package nextflow.latch

import java.nio.file.Path

import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import java.util.Base64

import nextflow.file.http.XPath

class LatchUtils {

    private static decoder = Base64.getDecoder()
    private static slurper = new JsonSlurper()

    static Object deserialize(String val) {
        def byteData = decoder.decode( val );

        try {
            def ois = new ObjectInputStream( new ByteArrayInputStream(  byteData ) );
            def deserializedVal = ois.readObject();
            ois.close();
            return deserializedVal
        } catch (Exception ignored) {
            String path = slurper.parse(byteData)
            return Path.of(path)
        }
    }

    static List deserializeParams(String serializedJson) {
        def serializedVals = slurper.parseText(serializedJson)

        def params = []
        for (String val: serializedVals) {
            params << deserialize(val)
        }

        return params
    }

    static List deserializeChannels(String serializedJson) {
        List<List<String>> serializedChannels = slurper.parseText(serializedJson)

        def channels = []
        for (def channel: serializedChannels) {
            channels << channel.collect({
                deserialize(it)
            })
        }
        return channels
    }

    static String serializeParam(Object value) {
        def baos = new ByteArrayOutputStream();

        println(value.getClass())

        try {
            def oos = new ObjectOutputStream( baos );
            oos.writeObject( value );
            oos.close();
        } catch (Exception ignored) { // for paths
            def s = new OutputStreamWriter(baos)
            s.write(value.toString())
        }

        return Base64.getEncoder().encodeToString(baos.toByteArray())
    }
}

