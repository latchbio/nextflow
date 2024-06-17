package nextflow.exception

import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors

/**
 * Exception thrown to signal a task execution failed due to a node termination,
 * likely due to a preemptive instance retirement
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@InheritConstructors
@CompileStatic

class K8sTimeoutException extends Exception implements ProcessRetryableException {
}
