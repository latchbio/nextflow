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

    boolean exists = false
    LDataNodeType type = null
    Long size

    LatchFileAttributes() {}

    LatchFileAttributes(String type, Long size) {
        exists = true
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
        return exists && type == LDataNodeType.obj
    }

    @Override
    boolean isDirectory() {
        return !exists || [LDataNodeType.dir, LDataNodeType.mount, LDataNodeType.account_root].contains(type)
    }

    @Override
    boolean isSymbolicLink() {
        return exists && type == LDataNodeType.link
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
