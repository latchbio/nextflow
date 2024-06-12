package nextflow.util

class DispatcherException extends Exception {
    int statusCode

    String error

    DispatcherException(String message, int statusCode, String error) {
        super("${message}; statusCode=${statusCode} error=${error}")
        this.statusCode = statusCode
        this.error = error
    }
}
