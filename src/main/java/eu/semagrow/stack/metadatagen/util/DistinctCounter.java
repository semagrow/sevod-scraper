package eu.semagrow.stack.metadatagen.util;

import java.io.*;
import java.util.UUID;

/**
 * Created by antonis on 18/5/2015.
 */
public class DistinctCounter {

    File file;
    FileWriter writer;

    public DistinctCounter(String prefix) {
        try {
            if (prefix == null) {
                file = File.createTempFile("metadatagen-", ".tmp");
            }
            else {
                file = File.createTempFile(prefix, ".tmp");
            }
        } catch (IOException e) {
            e.printStackTrace();
            file = null;
        }
    }

    public void add(String str) {
        openFile();
        try {
            writer.write(str + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
        closeFile();
    }

    public int getDistinctCount() {
        int count;

        String result = executeCommand("sort -u " + file.getAbsolutePath() + " 2>/dev/null | wc -l");

        if (result == "") {
            count = 0;
        }
        else {
            try {
                count = Integer.parseInt(result.substring(0,result.lastIndexOf('\n')));
            } catch (NumberFormatException e) {
                count = -1;
            }
        }

        return count;
    }

    public void close() {
        file.delete();
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
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void openFile() {
        try {
            writer = new FileWriter(file, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void closeFile() {
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
