package nextflow.util

import groovy.util.logging.Slf4j
import nextflow.file.http.GQLClient
import nextflow.file.http.GQLClient.GQLQueryException

@Slf4j
class ForchClient {
    private GQLClient client = new GQLClient(true)

    int submitTask(
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
                \$gpuType: String,
                \$gpus: Int!,
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

    String getTaskStatus(int forchTaskId) {
        Map res = client.execute("""
            query GetTaskStatus(\$taskId: BigInt!) {
                taskEvents(condition: {taskId: \$taskId}, orderBy: TIME_DESC, first: 1) {
                    nodes {
                        id
                        type
                        taskEventContainerExitedDatumById {
                            id
                            exitStatus
                        }
                    }
                }
            }
            """,
            [
                taskId: forchTaskId
            ]
        )["taskEvents"] as Map

        if (res == null)
            throw new RuntimeException("failed to get task events for ${forchTaskId}")

        List<Map> nodes = res["nodes"] as List<Map>
        if (nodes == null || nodes.size() == 0)
            return "queued"

        // todo(rahul): might be a good idea to throw this logic into a vac function so that we can easily update
        String eventType = nodes[0]["type"]
        if (eventType == "node-assigned")
            return "submitted"
        if (eventType == "container-created")
            return "running"
        if (eventType == "container-exited") {
            if ((nodes[0]["taskEventContainerExitedDatumById"]["exitStatus"] as int) == 0) {
                return "succeeded"
            } else {
                return "failed"
            }
        }

        return "queued"
    }

    int getTaskExitCode(int forchTaskId) {
        Map res = client.execute("""
            query GetTaskExitCode(\$taskId: BigInt!) {
                taskEvents(
                    condition: {taskId: \$taskId},
                    filter: {taskEventContainerExitedDatumByIdExists: true},
                    orderBy: TIME_DESC,
                    first: 1
                ) {
                    nodes {
                        id
                        type
                        taskEventContainerExitedDatumById {
                            id
                            exitStatus
                        }
                    }
                }
            }
            """,
            [
                taskId: forchTaskId
            ]
        )["taskEvents"] as Map

        if (res == null)
            throw new RuntimeException("failed to get exit code for ${forchTaskId}")

        List<Map> nodes = res["nodes"] as List<Map>
        if (nodes == null || nodes.size() == 0)
            return -1

        return nodes[0]["taskEventContainerExitedDatumById"]["exitStatus"] as int
    }
}
