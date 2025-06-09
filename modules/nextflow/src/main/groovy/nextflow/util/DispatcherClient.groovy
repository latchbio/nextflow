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
        if (debug) {
            return
        }

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


    int forchSubmitTask(
        String displayName,
        String image,
        List<String> entrypoint,
        int cpus,
        long memoryBytes
    ) {
        String resourceGroup = System.getenv("FORCH_RESOURCE_GROUP_ID")
        if (resourceGroup == null)
            throw new RuntimeException("unable to get resource group")

        String billingGroup = System.getenv("FORCH_BILLING_GROUP_ID")
        if (billingGroup == null)
            throw new RuntimeException("unable to get billing group")

        Map res = client.execute("""
            mutation CreateForchTask(
                \$displayName: String!,
                \$containerImage: String!,
                \$containerEntrypoint: [String]!,
                \$cpus: Int!,
                \$memoryBytes: BigInt!,
                \$dedicatedGpuType: String,
                \$dedicatedGpuCount: Int!,
                \$groupId: BigInt!,
                \$billedTo: BigInt!
            ) {
                createTask(
                    input: {
                        task: {
                            displayName: \$displayName,
                            containerImage: \$containerImage,
                            containerEntrypoint: \$containerEntrypoint,
                            dedicatedCpusetSize: \$cpus,
                            dedicatedMemoryBytes: \$memoryBytes,
                            allowInternetEgress: true,
                            dedicatedGpuType: \$gpuType,
                            dedicatedGpuCount: \$gpus,
                            groupId: \$groupId,
                            billedTo: \$billedTo
                        } 
                    }
                ) {
                    task {
                        id
                    }
                }
            }
            """,
            [
                "displayName" : displayName,
                "containerImage" : image,
                "containerEntrypoint" : entrypoint,
                "cpus" : cpus,
                "memoryBytes" : memoryBytes,
                "gpuType" : null,
                "gpus" : 0,
                "groupId": resourceGroup.toInteger(),
                "billedTo": billingGroup.toInteger()
            ]
        )["createTask"] as Map

        if (res == null)
            throw new RuntimeException("failed to create forch task")

        return ((res.task as Map).id as String).toInteger()
    }

    String forchGetTaskStatus(int forchTaskId) {
        List<Map> res = client.execute("""
            query GetTaskStatus(\$taskId: BigInt!) {
                taskEvents(condition: {taskId: \$taskId}, orderBy: TIME_DESC, first: 1) {
                    id
                    type
                    taskEventContainerExitedDatumById {
                        id
                        exitStatus
                    }
                }
            }
            """,
            [
                taskId: forchTaskId
            ]
        )["taskEvents"] as List<Map>

        if (res == null)
            throw new RuntimeException("failed to get task events for ${forchTaskId}")

        if (res.size() == 0)
            return "queued"

        // todo(rahul): might be a good idea to throw this logic into a vac function so that we can easily update
        String eventType = res[0]["type"]
        if (eventType == "node-assigned")
            return "submitted"
        if (eventType == "container-created")
            return "running"
        if (eventType == "container-exited") {
            if ((res[0]["taskEventContainerExitedDatumById"]["exitStatus"] as int) == 0) {
                return "succeeded"
            } else {
                return "failed"
            }
        }

        return "queued"
    }

    int forchGetExitCode(int forchTaskId) {
        List<Map> res = client.execute("""
            query GetTaskExitCode(\$taskId: BigInt!) {
                taskEvents(
                    condition: {taskId: \$taskId},
                    filter: {taskEventContainerExitedDatumByIdExists: true},
                    orderBy: TIME_DESC,
                    first: 1
                ) {
                    id
                    type
                    taskEventContainerExitedDatumById {
                        id
                        exitStatus
                    }
                }
            }
            """,
            [
                taskId: forchTaskId
            ]
        )["taskEvents"] as List<Map>

        if (res == null)
            throw new RuntimeException("failed to get exit code for ${forchTaskId}")

        if (res.size() == 0)
            return -1

        return res[0]["taskEventContainerExitedDatumById"]["exitStatus"] as int
    }
}
