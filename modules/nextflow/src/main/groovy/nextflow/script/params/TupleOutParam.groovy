/*
 * Copyright 2013-2024, Seqera Labs
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
 */

package nextflow.script.params

import groovy.transform.InheritConstructors
import nextflow.script.TokenEvalCall
import nextflow.script.TokenEnvCall
import nextflow.script.TokenFileCall
import nextflow.script.TokenPathCall
import nextflow.script.TokenStdoutCall
import nextflow.script.TokenValCall
import nextflow.script.TokenVar
/**
 * Model a set of process output parameters
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@InheritConstructors
class TupleOutParam extends BaseOutParam implements OptionalParam {

    protected List<BaseOutParam> inner = new ArrayList<>(10)

    String getName() { toString() }

    List<BaseOutParam> getInner() { inner }

    TupleOutParam clone() {
        final copy = (TupleOutParam)super.clone()
        copy.inner = new ArrayList<>(10)
        for( BaseOutParam p : inner ) {
            copy.inner.add(p.clone())
        }
        return copy
    }

    TupleOutParam bind(Object... obj ) {

        for( def item : obj ) {
            if( item instanceof TokenVar ) {
                throw new IllegalArgumentException("Unqualified output value declaration is not allowed - replace `tuple ${item.name},..` with `tuple val(${item.name}),..`")
            }
            else if( item instanceof TokenValCall ) {
                create(ValueOutParam).bind(item.val)
            }
            else if( item instanceof TokenEnvCall ) {
                create(EnvOutParam).bind(item.val)
            }
            else if( item instanceof TokenEvalCall ) {
                create(CmdEvalParam).bind(item.val)
            }
            else if( item instanceof GString ) {
                throw new IllegalArgumentException("Unqualified output path declaration is not allowed - replace `tuple \"$item\",..` with `tuple path(\"$item\"),..`")
            }
            else if( item instanceof TokenStdoutCall || item == '-'  ) {
                create(StdOutParam).bind('-')
            }
            else if( item instanceof String ) {
                throw new IllegalArgumentException("Unqualified output path declaration is not allowed - replace `tuple '$item',..` with `tuple path('$item'),..`")
            }
            else if( item instanceof TokenFileCall ) {
                // note that 'filePattern' can be a string or a GString
                create(FileOutParam).bind(item.target)
            }
            else if( item instanceof TokenPathCall ) {
                // note that 'filePattern' can be a string or a GString
                create(FileOutParam)
                        .setPathQualifier(true)
                        .setOptions(item.opts)
                        .bind(item.target)
            }
            else
                throw new IllegalArgumentException("Invalid `tuple` output parameter declaration -- item: ${item}")
        }

        return this
    }

    protected <T extends BaseOutParam> T create(Class<T> type) {
        type.newInstance(binding,inner,index)
    }

    @Override
    void lazyInit() {
        super.lazyInit()
        inner.each { opt ->
            if( opt instanceof FileOutParam ) opt.optional(this.optional)
        }
    }

}
