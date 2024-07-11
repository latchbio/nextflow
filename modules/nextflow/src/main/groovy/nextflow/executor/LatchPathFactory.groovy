package nextflow.executor

import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.ProviderMismatchException
import java.nio.file.ProviderNotFoundException
import java.nio.file.spi.FileSystemProvider

import nextflow.file.FileHelper
import nextflow.file.FileSystemPathFactory
import nextflow.file.http.LatchFileSystemProvider
import nextflow.file.http.LatchPath
import org.apache.commons.io.file.spi.FileSystemProviders

class LatchPathFactory extends FileSystemPathFactory {

    @Override
    protected Path parseUri(String uri) {
        if (!uri.startsWith("latch://")) return null

        List<FileSystemProvider> provs = FileSystemProvider.installedProviders()
        for (FileSystemProvider prov: provs) {
            if (prov.scheme != "latch") continue;

            return prov.getPath(new URI(null, null, uri, null, null))
        }

        return null
    }

    @Override
    protected String toUriString(Path path) {
        if (!(path instanceof LatchPath)) {
            return null
        }

        LatchPath lp = (LatchPath) path

        return lp.toUri().toString()
    }

    @Override
    protected String getBashLib(Path target) {
        if (target.scheme != "latch") {
            return null
        }

        return LatchBashLib.script()
    }

    @Override
    protected String getUploadCmd(String source, Path target) {
        if (target.scheme != "latch") {
            return null
        }

        def remoteParent = target.resolve(source).parent

        return "/opt/latch-env/bin/latch mkdirp ${target.toUriString()}/\"\$(dirname \"$source\")\"; /opt/latch-env/bin/latch cp --progress=none --verbose ${source} ${target.toUriString()}/$source"
    }
}
