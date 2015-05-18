package eu.semagrow.stack.metadatagen.util;

import java.io.*;
import java.util.UUID;

/**
 * Created by antonis on 18/5/2015.
 */
public class DistinctCounter {

    String fileName;
    File file;
    FileOutputStream is;
    OutputStreamWriter osw;
    Writer writer;

    public DistinctCounter() {
        fileName = "/tmp/metadatagen-" + UUID.randomUUID().toString() + ".tmp";
        createFile(fileName);
    }

    public void add(String str) {
        try {
            writer.write(str + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void clear() {
        deleteFile(fileName);
        createFile(fileName);
    }

    public int getDistinctCount() {
        int count;
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        String result = executeCommand("sort -u " + fileName + " | wc -l");

        if (result == "")
            count = 0;
        else
            count = Integer.valueOf(result.substring(0,result.indexOf('\n')));

        deleteFile(fileName);

        return count;
    }

    public void clearAll() {
        deleteAllFiles();
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////

    private String executeCommand(String command) {

        String[] cmd = { "/bin/sh", "-c", command };

        StringBuffer output = new StringBuffer();

        Process p;
        try {
            p = Runtime.getRuntime().exec(cmd);
            p.waitFor();
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));

            String line = "";
            while ((line = reader.readLine())!= null) {
                output.append(line + "\n");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return output.toString();
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////

    private void createFile(String path) {
        try {
            file = new File(path);
            if (!file.exists()) {
                file.createNewFile();
            }
            is = new FileOutputStream(file);
            osw = new OutputStreamWriter(is);
            writer = new BufferedWriter(osw);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void deleteFile(String path) {
        executeCommand("rm -f " + path);
    }

    private void deleteAllFiles() {
        executeCommand("rm -f /tmp/metadatagen-*");
    }
}
