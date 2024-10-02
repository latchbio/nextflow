package nextflow.util

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j
import nextflow.file.http.GQLClient

@Slf4j
class DispatcherClient {

    private GQLClient client = new GQLClient()

    int createProcessNode(String processName) {
        String executionToken = System.getenv("FLYTE_INTERNAL_EXECUTION_ID")
        if (executionToken == null)
                throw new RuntimeException("unable to get execution token")

        Map res = client.execute("""
            mutation CreateNode(\$executionToken: String!, \$name: String!) {
                createNfProcessNodeByExecutionToken(input: {argExecutionToken: \$executionToken, argName: \$name}) {
                    nodeId
                }
            }
            """,
            [
                executionToken: executionToken,
                name: processName,
            ]
        )["createNfProcessNodeByExecutionToken"] as Map

        if (res == null)
            throw new RuntimeException("failed to create remote process node for: processName=${processName}")

        return (res.nodeId as String).toInteger()
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
            throw new RuntimeException("failed to create remote process task for: processNodeId=${processNodeId} index=${index}")

        return ((res.nfTaskInfo as Map).id as String).toInteger()
    }

    int createTaskExecution(int taskId, int attemptIdx, String hash, String status = null) {
        Map res = client.execute("""
            mutation CreateTaskExecutionInfo(\$taskId: BigInt!, \$attemptIdx: BigInt!, \$hash: String!, \$status: TaskExecutionStatus!) {
                createNfTaskExecutionInfo(
                    input: {
                        nfTaskExecutionInfo: {
                            taskId: \$taskId,
                            attemptIdx: \$attemptIdx,
                            hash: \$hash,
                            status: \$status,
                            cpuLimitMillicores: "0",
                            memoryLimitBytes: "0",
                            ephemeralStorageLimitBytes: "0",
                            gpuLimit: "0"
                        }
                    }
                ) {
                    nfTaskExecutionInfo {
                        id
                    }
                }
            }
            """,
            [
                taskId: taskId,
                attemptIdx: attemptIdx,
                hash: hash,
                status: status == null ? 'UNDEFINED' : status,
            ]
        )["createNfTaskExecutionInfo"] as Map

        if (res == null)
            throw new RuntimeException("failed to create remote task execution for: taskId=${taskId} attempt=${attemptIdx} hash=${hash}")

        return ((res.nfTaskExecutionInfo as Map).id as String).toInteger()
    }

    void submitPod(int taskExecutionId, Map pod) {
        client.execute("""
            mutation UpdateTaskExecution(\$taskExecutionId: BigInt!, \$podSpec: String!) {
                updateNfTaskExecutionInfo(
                    input: {
                        id: \$taskExecutionId,
                        patch: {
                            status: QUEUED,
                            podSpec: \$podSpec
                        },
                    }
                ) {
                    clientMutationId
                }
            }
            """,
            [
                taskExecutionId: taskExecutionId,
                podSpec: JsonOutput.toJson(pod)
            ]
        )
    }

    void updateTaskStatus(int taskExecutionId, String status) {
        client.execute("""
            mutation UpdateTaskExecution(\$taskExecutionId: BigInt!, \$status: TaskExecutionStatus!) {
                updateNfTaskExecutionInfo(
                    input: {
                        id: \$taskExecutionId,
                        patch: {
                            status: \$status
                        },
                    }
                ) {
                    clientMutationId
                }
            }
            """,
            [
                taskExecutionId: taskExecutionId,
                status: status
            ]
        )
    }

    Map getTaskStatus(int taskExecutionId) {
        Map res = client.execute("""
            query GetNfExecutionTaskStatus(\$taskExecutionId: BigInt!) {
                nfTaskExecutionInfo(id: \$taskExecutionId) {
                    id
                    status
                    systemError
                }
            }
            """,
            [
                taskExecutionId: taskExecutionId
            ]
        )["nfTaskExecutionInfo"] as Map

        if (res == null)
            throw new RuntimeException("failed to get task execution status for: taskExecutionId=${taskExecutionId}")

        return res
    }
}
