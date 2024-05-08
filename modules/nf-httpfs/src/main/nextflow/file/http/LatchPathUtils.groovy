package nextflow.file.http

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
            return "Latch-SDK-Token ${tokenFile.text}"

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
            if (ws != null) {
                domain = "${ws}.account"
            }
        }

        return domain
    }

    private static String getCurrentWorkspaceFromLocalSetting() {
        String home = System.getProperty("user.home")
        File wsFile = new File("$home/.latch/workspace")

        if (!wsFile.exists())
            return null

        JsonSlurper slurper = new JsonSlurper()
        def wsJson = slurper.parse(new FileInputStream(wsFile))

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

        if (accInfo["user"] != null) {
            return accInfo["user"]["defaultAccount"] as String
        }

        return accInfo["id"] as String
    }
}
