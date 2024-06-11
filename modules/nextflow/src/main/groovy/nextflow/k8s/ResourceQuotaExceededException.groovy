package nextflow.k8s

class ResourceQuotaExceededException extends Exception {
    ResourceQuotaExceededException(String message) {
        super(message)
    }
}
