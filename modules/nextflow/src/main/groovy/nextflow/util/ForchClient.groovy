package nextflow.util

import groovy.util.logging.Slf4j
import nextflow.file.http.GQLClient

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
        String resourceGroup = System.getenv("forch_resource_group_id")
        if (resourceGroup == null)
            throw new RuntimeException("unable to get resource group")

        String billingGroup = System.getenv("forch_billing_group_id")
        if (billingGroup == null)
            throw new RuntimeException("unable to get billing group")

        String nfsServerTaskId = System.getenv("nfs_server_task_id")
        if (nfsServerTaskId == null)
            throw new RuntimeException("unable to get NFS server task id")

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
                \$billedTo: BigInt!,
                \$nfsServerTaskId: BigInt!
            ) {
                nfCreateForchTask(
                    input: {
                        argDisplayName: \$displayName,
                        argContainerImage: \$containerImage,
                        argContainerEntrypoint: \$containerEntrypoint,
                        argCpus: \$cpus,
                        argMemoryBytes: \$memoryBytes,
                        argGpuType: \$gpuType,
                        argGpus: \$gpus,
                        argGroupId: \$groupId,
                        argBilledTo: \$billedTo,
                        argNfsServerTaskId: \$nfsServerTaskId
                    }
                ) {
                    resTaskId
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
                "billedTo": billingGroup.toInteger(),
                "nfsServerTaskId": nfsServerTaskId,
            ]
        )["nfCreateForchTask"] as Map

        if (res == null)
            throw new RuntimeException("failed to create forch task")

        return (res.resTaskId as String).toInteger()
    }

    String getTaskStatus(int forchTaskId) {
        Map res = client.execute("""
            query GetTaskStatus(\$taskId: BigInt!) {
                taskStatus(argTaskId: \$taskId)
            }
            """,
            [
                taskId: forchTaskId
            ]
        ) as Map

        if (res == null)
            throw new RuntimeException("failed to get task status for ${forchTaskId}")

        return res["nfForchTaskStatus"]
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
