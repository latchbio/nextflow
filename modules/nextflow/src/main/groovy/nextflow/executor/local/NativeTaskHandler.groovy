/*
 * Copyright 2013-2023, Seqera Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package nextflow.executor.local

import nextflow.util.DispatcherClient

import java.util.concurrent.Callable
import java.util.concurrent.Future

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.executor.Executor
import nextflow.processor.TaskHandler
import nextflow.processor.TaskRun
import nextflow.processor.TaskStatus

/**
 * Executes a native piece of groovy code
 */
@Slf4j
@CompileStatic
class NativeTaskHandler extends TaskHandler {

    Future<Object> result

    private Session session

    private Executor executor

    private DispatcherClient dispatcherClient

    private class TaskSubmit implements Callable {

        final TaskRun task

        TaskSubmit( TaskRun obj ) { task = obj }

        @Override
        Object call() throws Exception {
            try  {
                return task.code.call()
            }
            catch( Throwable error ) {
                return error
            }
            finally {
                executor.getTaskMonitor().signal()
            }
        }
    }

    protected NativeTaskHandler(TaskRun task, Executor executor) {
        super(task)
        this.executor = executor
        this.session = executor.session
        this.dispatcherClient = executor.dispatcherClient
    }


    @Override
    void submit() {
        // submit for execution by using session executor service
        // it returns an error when everything is OK
        // of the exception throw in case of error
        result = session.getExecService().submit(new TaskSubmit(task))
        dispatcherClient.updateTaskStatus(taskExecutionId, 'INITIALIZING')
        status = TaskStatus.SUBMITTED
    }

    @Override
    boolean checkIfRunning() {
        if( isSubmitted() && result != null ) {
            if (status != TaskStatus.RUNNING)
                dispatcherClient.updateTaskStatus(taskExecutionId, 'RUNNING')
            status = TaskStatus.RUNNING
            return true
        }

        return false
    }

    @Override
    boolean checkIfCompleted() {
        if( isRunning() && result.isDone() ) {
            status = TaskStatus.COMPLETED
            if( result.get() instanceof Throwable ) {
                dispatcherClient.updateTaskStatus(taskExecutionId, 'FAILED')
                task.error = (Throwable)result.get()
            }
            else {
                dispatcherClient.updateTaskStatus(taskExecutionId, 'SUCCEEDED')
                task.stdout = result.get()
            }
            return true
        }
        return false
    }

    @Override
    void kill() {
        if( result ) result.cancel(true)
    }

}
