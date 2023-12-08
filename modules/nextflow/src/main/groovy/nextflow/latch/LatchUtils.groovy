package nextflow.latch

import groovy.json.JsonSlurper
import java.util.Base64

class LatchUtils {

    static List deserializeParams(String serializedJson) {

        def slurper = new groovy.json.JsonSlurper()
        def serializedVals = slurper.parseText(serializedJson)
        def decoder = Base64.getDecoder()

        def params = []
        for (String val: serializedVals) {
            def byteData = decoder.decode( val );
            def ois = new ObjectInputStream( new ByteArrayInputStream(  byteData ) );
            def deserializedVal = ois.readObject();
            ois.close();
            params << deserializedVal
        }

        return params
    }

    static List deserializeChannels(String serializedJson) {

        def slurper = new groovy.json.JsonSlurper()
        def serializedChannels = slurper.parseText(serializedJson)
        def decoder = Base64.getDecoder()

        def channels = []
        for (List channel: serializedChannels) {
            channels << channel.collect({
                def byteData = decoder.decode( it );
                def ois = new ObjectInputStream( new ByteArrayInputStream(  byteData ) );
                def deserializedVal = ois.readObject();
                ois.close();
                return deserializedVal
            })
        }
        return channels
    }

    static String serializeParam(Object value) {
        def baos = new java.io.ByteArrayOutputStream();
        def oos = new java.io.ObjectOutputStream( baos );
        oos.writeObject( value );
        oos.close();
        return java.util.Base64.getEncoder().encodeToString(baos.toByteArray())
    }
}

