package nextflow.util

import groovy.json.JsonOutput
import groovy.util.logging.Slf4j
import nextflow.file.http.GQLClient
import nextflow.file.http.GQLClient.GQLQueryException

@Slf4j
class DispatcherClient {

    private GQLClient client = new GQLClient()

    public boolean debug = System.getenv("LATCH_NF_DEBUG") == "true"

    int createProcessNode(String processName) {
        if (debug) {
            return 1
        }

        String executionToken = System.getenv("FLYTE_INTERNAL_EXECUTION_ID")
        if (executionToken != null) {
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

        String executionId = System.getenv("forch_execution_id")
        if (executionId != null) {
            Map res = client.execute("""
                mutation CreateNode(\$executionId: BigInt!, \$name: String!) {
                    createNfProcessNode(input: {nfProcessNode: {executionId: \$executionId, name: \$name } }) {
                        nfProcessNode {
                            id
                        }
                    }
                }
                """,
                [
                    executionId: executionId,
                    name: processName,
                ]
            )["createNfProcessNode"] as Map

            if (res == null || res["nfProcessNode"] == null)
                throw new RuntimeException("failed to create remote process node for: processName=${processName}")

            return (res["nfProcessNode"]["id"] as String).toInteger()
        }

        throw new RuntimeException("failed to create process node: unable to get source execution")
    }

    void closeProcessNode(int nodeId, int numTasks) {
        if (debug) {
            return
        }

        client.execute("""
            mutation UpdateTaskInfo(\$nodeId: BigInt!, \$numTasks: BigInt!) {
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
        if (debug) {
            return
        }

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
        if (debug) {
            return 1
        }

        try {
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
        } catch (GQLQueryException e) {
            if (!e.message.contains("duplicate key value violates unique constraint")) {
                throw e
            }
        }

        Map res = client.execute("""
            query GetNfTaskInfo(\$index: BigInt!, \$processNodeId: BigInt!) {
                nfTaskInfoByProcessNodeIdAndIndex(index: \$index, processNodeId: \$processNodeId) {
                    id
                }
            }
            """,
            [
                processNodeId: processNodeId,
                index: index,
            ]
        )["nfTaskInfoByProcessNodeIdAndIndex"] as Map

        if (res == null)
            throw new RuntimeException("failed to get task id for: processNodeId=${processNodeId} index=${index}")

        return (res.id as String).toInteger()
    }

    int createTaskExecution(int taskId, int attemptIdx, String hash, String status = null) {
        if (debug) {
            return 1
        }

        String forchExecutionId = System.getenv("forch_execution_id")
        if (forchExecutionId != null) {
            try {
                Map res = client.execute("""
                    mutation CreateForchTaskExecutionInfo(\$taskId: BigInt!, \$attemptIdx: BigInt!, \$cached: Boolean!, \$hash: String) {
                        createNfForchTaskExecutionInfo(
                            input: {
                                nfForchTaskExecutionInfo: {
                                    taskId: \$taskId,
                                    attemptIdx: \$attemptIdx,
                                    cached: \$cached,
                                    hash: \$hash
                                }
                            }
                        ) {
                            nfForchTaskExecutionInfo {
                                id
                            }
                        }
                    }
                    """,
                    [
                        taskId: taskId,
                        attemptIdx: attemptIdx,
                        cached: status == 'SKIPPED',
                        hash: hash,
                    ]
                )["createNfForchTaskExecutionInfo"] as Map

                if (res == null)
                    throw new RuntimeException("failed to create remote task execution for: taskId=${taskId} attempt=${attemptIdx} hash=${hash}")

                return ((res.nfForchTaskExecutionInfo as Map).id as String).toInteger()
            } catch (GQLQueryException e) {

                // note(rahul): the gql client uses the HTTP Retry Client. As a result, it may retry a request after
                // successfully committing the row to the DB (for example, if the connection fails)
                if (!e.message.contains("duplicate key value violates unique constraint")) {
                    throw e
                }
            }

            Map res = client.execute("""
                query GetNfForchTaskExecutionInfo(\$taskId: BigInt!, \$attemptIdx: BigInt!) {
                    nfForchTaskExecutionInfoByTaskIdAndAttemptIdx(attemptIdx: \$attemptIdx, taskId: \$taskId) {
                        id
                    }
                }
                """,
                [
                    taskId: taskId,
                    attemptIdx: attemptIdx,
                ]
            )["nfForchTaskExecutionInfoByTaskIdAndAttemptIdx"] as Map

            if (res == null)
                throw new RuntimeException("failed to get forch task execution id for: taskId=${taskId} attemptIdx=${attemptIdx}")

            return (res.id as String).toInteger()
        }

        try {
            Map res = client.execute("""
                mutation CreateTaskExecutionInfo(\$taskId: BigInt!, \$attemptIdx: BigInt!, \$hash: String, \$status: TaskExecutionStatus!) {
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
        } catch (GQLQueryException e) {

            // note(rahul): the gql client uses the HTTP Retry Client. As a result, it may retry a request after
            // successfully committing the row to the DB (for example, if the connection fails)
            if (!e.message.contains("duplicate key value violates unique constraint")) {
                throw e
            }
        }

        Map res = client.execute("""
            query GetNfTaskExecutionInfo(\$taskId: BigInt!, \$attemptIdx: BigInt!) {
                nfTaskExecutionInfoByTaskIdAndAttemptIdx(attemptIdx: \$attemptIdx, taskId: \$taskId) {
                    id
                }
            }
            """,
            [
                taskId: taskId,
                attemptIdx: attemptIdx,
            ]
        )["nfTaskExecutionInfoByTaskIdAndAttemptIdx"] as Map

        if (res == null)
            throw new RuntimeException("failed to get task execution id for: taskId=${taskId} attemptIdx=${attemptIdx}")

        return (res.id as String).toInteger()
    }

    void submitPod(int taskExecutionId, Map pod) {
        if (debug) return

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
        if (debug) {
            return
        }

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
        if (debug) {
            return null
        }

        Map res = client.execute("""
            query GetNfExecutionTaskStatus(\$taskExecutionId: BigInt!) {
                nfTaskExecutionInfo(id: \$taskExecutionId) {
                    id
                    status
                    systemError
                    runtimeError
                    exitCode
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

    void updateForchTaskId(int taskExecutionId, int forchTaskId) {
        if (debug) {
            return
        }

        client.execute("""
            mutation UpdateTaskExecution(\$taskExecutionId: BigInt!, \$forchTaskId: BigInt!) {
                updateNfForchTaskExecutionInfo(
                    input: {
                        id: \$taskExecutionId,
                        patch: {
                            forchTaskId: \$forchTaskId
                        },
                    }
                ) {
                    clientMutationId
                }
            }
            """,
            [
                taskExecutionId: taskExecutionId,
                forchTaskId: forchTaskId
            ]
        )
    }
}
