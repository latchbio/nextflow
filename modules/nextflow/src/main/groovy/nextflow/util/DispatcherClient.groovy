package nextflow.util

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j

@Slf4j
class DispatcherClient {
    final private String DISPATCH_DOMAIN = 'nf-dispatcher-service.flyte.svc.cluster.local'

    private String authToken

    private Boolean debug

    DispatcherClient() {
        this.debug = System.getenv("LATCH_NF_DEBUG") != null
    }

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
                sleep(5000 * i)
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

    int createProcessNode(String processName) {
        if (debug)
            return 0

        def resp = requestWithRetry(
            'POST',
            'create-process',
            [
                name: processName
            ]
        )

        def data = (Map) new JsonSlurper().parseText(resp)
        return (int) data.id
    }

    void closeProcessNode(int nodeId, int numTasks) {
        if (debug)
            return

        requestWithRetry(
            'POST',
            'close-process',
            [
                id: nodeId,
                num_tasks: numTasks
            ]
        )
    }

    void createProcessEdge(int from, int to) {
        if (debug)
            return

        requestWithRetry(
            'POST',
            'create-edge',
            [
                start_node_id: from,
                end_node_id: to
            ]
        )
    }

    int createProcessTask(int processNodeId, int index, String tag) {
        if (debug)
            return 0

        def resp = requestWithRetry(
            'POST',
            'create-task',
            [
                process_node_id: processNodeId,
                index: index,
                tag: tag
            ]
        )

        def data = (Map) new JsonSlurper().parseText(resp)
        return (int) data.id
    }

    int createTaskExecution(int taskId, int attemptIdx, String status = null) {
        if (debug)
            return 0

        def resp = requestWithRetry(
            'POST',
            'create-task-execution',
            [
                task_id: taskId,
                attempt_idx: attemptIdx,
                status: status
            ]
        )

        def data = (Map) new JsonSlurper().parseText(resp)
        return (int) data.id
    }

    String dispatchPod(int taskExecutionId, Map pod) {
        if (debug)
            return ""

        def resp = requestWithRetry(
            'POST',
            'submit',
            [
                task_execution_id: taskExecutionId,
                pod: JsonOutput.toJson(pod),
            ]
        )

        def data = (Map) new JsonSlurper().parseText(resp)
        return data.name
    }

    void updateTaskStatus(int taskExecutionId, String status) {
        if (debug)
            return

        requestWithRetry(
            'POST',
            'status',
            [
                task_execution_id: taskExecutionId,
                status: status,
            ]
        )
    }
}
