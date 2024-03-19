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
 */

package nextflow.cli

import org.yaml.snakeyaml.DumperOptions
import org.yaml.snakeyaml.Yaml

import java.nio.file.Path
import java.nio.file.Paths

import com.beust.jcommander.Parameter
import com.beust.jcommander.Parameters
import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.config.ConfigBuilder
import nextflow.exception.AbortOperationException
import nextflow.plugin.Plugins
import nextflow.scm.AssetManager
import nextflow.util.ConfigHelper
/**
 *  Prints the pipeline configuration
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
@Parameters(commandDescription = "Print a project configuration")
class CmdConfig extends CmdBase {

    static final public NAME = 'config'

    static final public List<String> VALID_FORMATS = ['properties', 'flat', 'yaml']

    @Parameter(description = 'project name')
    List<String> args = []

    @Parameter(names=['-a','-show-profiles'], description = 'Show all configuration profiles')
    boolean showAllProfiles

    @Parameter(names=['-profile'], description = 'Choose a configuration profile')
    String profile

    @Parameter(names = '-format', description = 'Prints config using the specified format')
    String format

    @Parameter(names = '-sort', description = 'Sort config attributes')
    boolean sort

    @Parameter(names = '-value', description = 'Print the value of a config option, or fail if the option is not defined')
    String printValue

    @Parameter(names = '-output', description = 'Write config to the specified path')
    String outputPath

    @Override
    String getName() { NAME }

    private OutputStream stdout = System.out

    @Override
    void run() {
        Plugins.init()
        Path base = null
        if( args ) base = getBaseDir(args[0])
        if( !base ) base = Paths.get('.')

        if( profile && showAllProfiles )
            throw new AbortOperationException("Option `-profile` conflicts with option `-show-profiles`")

        if( format && printValue )
            throw new AbortOperationException("Option `-format` and `-value` conflicts")

        if( format && !VALID_FORMATS.contains(format) )
            throw new AbortOperationException("Not a valid output format: $format -- It must be one of the following: ${VALID_FORMATS.join(',')}")

        final builder = new ConfigBuilder()
                .setShowClosures(true)
                .showMissingVariables(true)
                .setOptions(launcher.options)
                .setBaseDir(base)
                .setCmdConfig(this)

        final config = builder.buildConfigObject()

        OutputStream stream = stdout
        if( outputPath ) {
            stream = new FileOutputStream(outputPath)
        }

        if( format ) {
            if (format == 'properties')
                printProperties0(config, stream)
            else if (format == 'flat')
                printFlatten0(config, stream)
            else if (format == 'yaml')
                printYaml0(config, stream)
            else
                throw new AbortOperationException("Format not supported: $format")
        }
        else if( printValue ) {
            printValue0(config, printValue, stream)
        }
        else {
            printCanonical0(config, stream)
        }


        for( String msg : builder.warnings )
            log.warn(msg)
    }

    @PackageScope void printYaml0(ConfigObject config, OutputStream output) {
        def options = new DumperOptions()
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
        final String yaml = new Yaml(options).dump(config)
        output << yaml
    }

    /**
     * Prints a {@link ConfigObject} using Java {@link Properties} in canonical format
     * ie. any nested config object is printed within curly brackets
     *
     * @param config The {@link ConfigObject} representing the parsed workflow configuration
     * @param output The stream where output the formatted configuration notation
     */
    @PackageScope void printCanonical0(ConfigObject config, OutputStream output) {
        output << ConfigHelper.toCanonicalString(config, sort)
    }

    /**
     * Prints a {@link ConfigObject} using Java {@link Properties} format
     *
     * @param config The {@link ConfigObject} representing the parsed workflow configuration
     * @param output The stream where output the formatted configuration notation
     */
    @PackageScope void printProperties0(ConfigObject config, OutputStream output) {
        output << ConfigHelper.toPropertiesString(config, sort)
    }

    /**
     * Prints a property of a {@link ConfigObject}.
     *
     * @param config The {@link ConfigObject} representing the parsed workflow configuration
     * @param name The {@link String} representing the property name using dot notation
     * @param output The stream where output the formatted configuration notation
     */
    @PackageScope void printValue0(ConfigObject config, String name, OutputStream output) {
        final map = config.flatten()
        if( !map.containsKey(name) )
            throw new AbortOperationException("Configuration option '$name' not found")

        output << map.get(name).toString() << '\n'
    }

    /**
     * Prints a {@link ConfigObject} using properties dot notation.
     * String values are enclosed in single quote characters.
     *
     * @param config The {@link ConfigObject} representing the parsed workflow configuration
     * @param output The stream where output the formatted configuration notation
    */
    @PackageScope void printFlatten0(ConfigObject config, OutputStream output) {
        output << ConfigHelper.toFlattenString(config, sort)
    }

    /**
     * Prints the {@link ConfigObject} configuration object using the default notation
     *
     * @param config The {@link ConfigObject} representing the parsed workflow configuration
     * @param output The stream where output the formatted configuration notation
     */
    @PackageScope void printDefault0(ConfigObject config, OutputStream output) {
        def writer = new PrintWriter(output,true)
        config.writeTo( writer )
    }


    Path getBaseDir(String path) {

        def file = Paths.get(path)
        if( file.isDirectory() )
            return file

        if( file.exists() ) {
            return file.parent ?: Paths.get('/')
        }

        final manager = new AssetManager(path)
        manager.isLocal() ? manager.localPath.toPath() : manager.configFile?.parent

    }

}
