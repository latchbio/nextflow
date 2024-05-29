package nextflow.file.http

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileTime

class LatchFileAttributes implements BasicFileAttributes {
    static enum LDataNodeType {
        account_root,
        dir,
        obj,
        mount,
        link

        static LDataNodeType fromString(String s) {
            s = s.toLowerCase()

            if (s == "account_root") return account_root
            if (s == "dir") return dir
            if (s == "obj") return obj
            if (s == "mount") return mount
            if (s == "link") return link

            throw new UnsupportedOperationException("Not a valid LDataNodeType: $s")
        }
    }

    LDataNodeType type = null
    Long size

    LatchFileAttributes(String type, Long size) {
        this.type = LDataNodeType.fromString(type)
        this.size = size
    }

    @Override
    FileTime lastModifiedTime() {
        return null
    }

    @Override
    FileTime lastAccessTime() {
        return null
    }

    @Override
    FileTime creationTime() {
        return null
    }

    @Override
    boolean isRegularFile() {
        return type == LDataNodeType.obj
    }

    @Override
    boolean isDirectory() {
        return [LDataNodeType.dir, LDataNodeType.mount, LDataNodeType.account_root].contains(type)
    }

    @Override
    boolean isSymbolicLink() {
        return type == LDataNodeType.link
    }

    @Override
    boolean isOther() {
        return false
    }

    @Override
    long size() {
        return size
    }

    @Override
    Object fileKey() {
        return null
    }
}
