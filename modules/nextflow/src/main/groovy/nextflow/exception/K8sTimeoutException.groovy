package nextflow.exception

import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors

/**
 * Exception thrown to signal a task execution failed due to a k8s timeout
 *
 * @author Rahul Desai <rahul@latch.bio>
 */
@InheritConstructors
@CompileStatic

class K8sTimeoutException extends Exception implements ProcessRetryableException {
}
