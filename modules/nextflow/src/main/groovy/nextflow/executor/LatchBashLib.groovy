package nextflow.executor

import nextflow.Global
import nextflow.Session

class LatchBashLib extends BashFunLib<LatchBashLib> {
    @Override
    String render() {
        return super.render()
    }


    static String script() {
        return new LatchBashLib().render()
    }
}
