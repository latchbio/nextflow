package nextflow.file.http

import groovy.json.JsonException
import groovy.json.JsonSlurper

class LatchPathUtils {

    static class UnauthenticatedException extends Exception {}

    static String getAuthHeader() {
        def flyteToken = System.getenv("FLYTE_INTERNAL_EXECUTION_ID")
        if (flyteToken != null)
            return "Latch-Execution-Token $flyteToken"

        String home = System.getProperty("user.home")
        File tokenFile = new File("$home/.latch/token")
        if (tokenFile.exists())
            return "Latch-SDK-Token ${tokenFile.text.strip()}"

        throw new UnauthenticatedException()
    }

    static getCurrentWorkspace() {
        String local = getCurrentWorkspaceFromLocalSetting()
        if (local != null)
            return local

        return getCurrentWorkspaceFromNetwork()
    }

    static getDomain(URI uri) {
        String domain = uri.authority
        if (domain == null || domain == "") {
            String ws = getCurrentWorkspace()
            if (ws == null)
                throw new RuntimeException("failed to get current workspace")

            domain = "${ws}.account"
        }

        return domain
    }

    private static String getCurrentWorkspaceFromLocalSetting() {
        String home = System.getProperty("user.home")
        File wsFile = new File("$home/.latch/workspace")

        if (!wsFile.exists())
            return null

        def wsJson = null
        try {
            JsonSlurper slurper = new JsonSlurper()
            def f = new FileInputStream(wsFile)
            wsJson = slurper.parse(f)
            f.close()
        } catch (JsonException ignored) {
            return null
        }

        try {
            return wsJson["workspace_id"]
        } catch (MissingPropertyException ignored) {
            return wsJson as String
        }
    }

    private static String getCurrentWorkspaceFromNetwork() {
        def client = new GQLClient()
        Map accInfo = client.execute("""
           query DefaultAccountQuery {
                accountInfoCurrent {
                    id
                    user {
                        defaultAccount
                    }
                }
            } 
        """)["accountInfoCurrent"] as Map

        if (accInfo == null)
            return null

        if (accInfo.user != null)
            return ((Map) accInfo.user).defaultAccount as String

        return accInfo.id as String
    }
}
