package nextflow.util

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j
import nextflow.file.http.GQLClient

@Slf4j
class DispatcherClient {

    private GQLClient client = new GQLClient()

    int createProcessNode(String processName) {
        Map res = client.execute("""
            mutation CreateNode(\$executionToken: String!, \$name: String!) {
                createNfProcessNodeByExecutionToken(input: {argExecutionToken: \$executionToken, argName: \$name}) {
                    bigInt
                }
            }
            """,
            [
                // TODO(rahul): inject nf execution info id into env variable and use that for creating process nodes
                executionToken: executionToken,
                name: processName,
            ]
        )["createNfProcessNodeByExecutionToken"] as Map

        if (res == null)
            throw new RuntimeException("failed to create remote process node")

        return (res.bigInt as String).toInteger()
    }

    void closeProcessNode(int nodeId, int numTasks) {
        client.execute("""
            mutation CreateTaskInfo(\$nodeId: BigInt!, \$numTasks: BigInt!) {
                updateNfProcessNode(
                    input: {
                        id: \$nodeId,
                        patch: {
                            numTasks: \$numTasks
                        }
                    }
                ) {
                    clientMutationId
                }
            }
            """,
            [
                nodeId: nodeId,
                numTasks: numTasks
            ]
        )
    }

    void createProcessEdge(int from, int to) {
        client.execute("""
            mutation CreateEdge(\$startNode: BigInt!, \$endNode: BigInt!) {
                createNfProcessEdge(
                    input: {
                        nfProcessEdge: {
                            startNode: \$startNode,
                            endNode: \$endNode
                        }
                    }
                ) {
                    clientMutationId
                }
            }
            """,
            [
                startNode: from,
                endNode: to,
            ]
        )
    }

    int createProcessTask(int processNodeId, int index, String tag) {
        Map res = client.execute("""
            mutation CreateTaskInfo(\$processNodeId: BigInt!, \$index: BigInt!, \$tag: String) {
                createNfTaskInfo(
                    input: {
                        nfTaskInfo: {
                            processNodeId: \$processNodeId,
                            index: \$index,
                            tag: \$tag
                        }
                    }
                ) {
                    nfTaskInfo {
                        id
                    }
                }
            }
            """,
            [
                processNodeId: processNodeId,
                index: index,
                tag: tag,
            ]
        )["createNfTaskInfo"] as Map

        if (res == null)
            throw new RuntimeException("failed to create remote process task")

        return ((res.nfTaskInfo as Map).id as String).toInteger()
    }

    int createTaskExecution(int taskId, int attemptIdx, String status = null) {
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

    void submitPod(int taskExecutionId, Map pod) {
        def resp = requestWithRetry(
            'POST',
            'submit',
            [
                task_execution_id: taskExecutionId,
                pod: JsonOutput.toJson(pod),
            ]
        )
    }

    String getTaskStatus(int taskExecutionId) {
        requestWithRetry(
            'POST',
            'status',
            [
                task_execution_id: taskExecutionId,
                status: status,
            ]
        )
    }

    void abortTask(int taskExecutionId) {
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
