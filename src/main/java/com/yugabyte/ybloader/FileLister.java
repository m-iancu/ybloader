package com.yugabyte.ybloader;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;

public class FileLister {

    private final Pattern filePattern;

    public FileLister(String pattern) {
        filePattern = Pattern.compile(pattern);
    }

    public List<File> listFiles() {
        File directory = new File(".");
        File[] files = directory.listFiles();
        if (files == null || files.length < 1) {
            return Collections.emptyList();
        }
        List<File> result = Lists.newLinkedList();
        for (File file: files) {
            Matcher matcher = filePattern.matcher(file.getName());
            if (matcher.matches()) {
                result.add(file);
            }
        }
        return result;
    }
}
