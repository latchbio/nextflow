package nextflow.util

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j

@Slf4j
class DispatcherClient {
    final private String DISPATCH_DOMAIN = 'nf-dispatcher-service.flyte.svc.cluster.local'

    private String authToken

    private String requestWithRetry(String method, String path, Map body, int retries = 3) {
        if (authToken == null) {
            def token = System.getenv('FLYTE_INTERNAL_EXECUTION_ID')
            if (token == null)
                throw new RuntimeException("failed to get latch execution token")
            authToken = token
        }

        def statusCode = -1
        def error = ""
        for (int i = 0; i < retries; i++) {
            if (i != 0) {
                log.warn "${path} request failed ${i}/${retries}, retrying: status_code=${statusCode} error=${error}"
                sleep(5000)
            }

            def url = new URL("http://${DISPATCH_DOMAIN}/${path}")
            def conn = (HttpURLConnection) url.openConnection()
            conn.setRequestMethod(method)
            conn.setRequestProperty('Content-Type', 'application/json')
            conn.setRequestProperty('Authorization', "Latch-Execution-Token ${authToken}")

            try {
                conn.setDoOutput(true)
                conn.outputStream.withWriter { writer ->
                    writer << JsonOutput.toJson(body)
                }

                statusCode = conn.getResponseCode()
                if (conn.errorStream != null)
                    error = conn.errorStream.getText()

                if (statusCode != 200) {
                    if (statusCode >= 500)
                        continue
                    break
                }

                return conn.getInputStream().getText()
            } catch (ConnectException  e) {
                error = e.toString()
            } finally {
                conn.disconnect()
            }
        }

        throw new RuntimeException("${path} request failed after ${retries} attempts: status_code=${statusCode} error=${error}")
    }

    String dispatchPod(int taskId, int attemptIdx, Map pod) {
        def resp = requestWithRetry(
            'POST',
            'submit',
            [
                pod: JsonOutput.toJson(pod),
                task_id: taskId,
                attempt_idx: attemptIdx,
            ]
        )

        def data = (Map) new JsonSlurper().parseText(resp)
        return data.name
    }

    int createProcessNode(String processName) {
        def resp = requestWithRetry(
            'POST',
            'create-node',
            [
                name: processName
            ]
        )

        def data = (Map) new JsonSlurper().parseText(resp)
        return (int) data.id
    }

    void addProcessEdge(int from, int to) {
        requestWithRetry(
            'POST',
            'create-edge',
            [
                from: from,
                to: to
            ]
        )
    }

    int createProcessTask(int processNodeId, int index) {
        def resp = requestWithRetry(
            'POST',
            'create-task',
            [
                process_node_id: processNodeId,
                index: index
            ]
        )

        def data = (Map) new JsonSlurper().parseText(resp)
        return (int) data.id
    }

    void updateTaskStatus(int taskId, int attemptIdx, String status) {
        requestWithRetry(
            'POST',
            'status',
            [
                task_id: taskId,
                attempt_idx: attemptIdx,
                status: status,
            ]
        )
    }
}
