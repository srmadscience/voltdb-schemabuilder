/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
package org.voltdb.voltutil.schemabuilder;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltcore.network.VoltPort;

/**
 * Utility class to build a schema.
 * 
 */
public final class VoltDBSchemaBuilder {

    // Used to spot correct error message
    private static final String PROCEDURE = "Procedure ";
    private static final String WAS_NOT_FOUND = " was not found";

    Client voltClient;

    // name of JAR file to create to store content.
    private String jarFileName;

    // DDL statements, in a rational order..
    private String[] ddlStatements;

    // procedure creation statements, either from a CLASS file or a SQL
    // statement
    private String[] procStatements;

    // Procedure class file names
    private String[] procClassNames;

    // Extra ZIP files we need loaded..
    private String[] zipFiles;

    // Misc other classes we need.
    private String[] otherClasses;

    // The package name for our stored procedures.
    String procPackageName;

    // In order to test if the schema already exists we issue a call
    // to a read only procedure called 'testprocname' and pass in the
    // parameters 'testParams'
    String testProcName;
    Object[] testParams;

    /**
     * As part of the schema creation process we generate a JAR file containing
     * Java procedures. 'deleteFiles' controls whether this file is deleted at 
     * the end of the process.
     */
    private boolean deleteFiles = true;
    
    /**
     * Date format for log messages
     */
    static SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    /**
     * Utility class to build a schema.
     * 
     * @param ddlStatements
     *            Array containing DDL statements in a logical order.
     * @param procStatements
     *            Array containing procedure defintions
     * @param zipFiles
     *            Array containing names of zip files we want to include
     * @param jarFileName
     *            name of Jar file we create
     * @param voltClient
     *            handle to Volt Client. Note we never close it, as we assume somebody else
     *            needs it for something...
     * @param procPackageName
     *            Java package our stored procs are in
     * @param testProcName
     *            name of test procedure
     * @param testParams
     *            Array of params for test procedure
     * @param otherClasses
     *            Other java classes we need to load
     */
    public VoltDBSchemaBuilder(String[] ddlStatements, String[] procStatements, String[] zipFiles, String jarFileName,
            Client voltClient, String procPackageName, String testProcName, Object[] testParams,
            String[] otherClasses) {
        super();
        this.ddlStatements = ddlStatements;
        this.procStatements = procStatements;
        this.voltClient = voltClient;
        this.procPackageName = procPackageName;
        this.zipFiles = zipFiles;
        this.jarFileName = jarFileName;
        this.testProcName = testProcName;
        this.testParams = testParams;
        this.otherClasses = otherClasses;

        procClassNames = getProcClassNames(procStatements);
    }

    /**
     * Load classes and DDL
     * 
     * @throws IOException
     * @throws MissingResourceException we can't find a class that's mentioned
     * @throws CreatedFileTooBigException JAR file is > 45MB
     * @throws ProcCallException Something went wrong at the DB level when we called UpdateClasses
     * @throws FailedToUpdateClassesException The DB call to UpdateClasses worked but didn't return SUCCESS.
     * @throws FailedToCreateDDLException Something wrong with DDL syntax.
     */
    public synchronized boolean loadClassesAndDDLIfNeeded() throws IOException, MissingResourceException,
            CreatedFileTooBigException, FailedToUpdateClassesException, ProcCallException, FailedToCreateDDLException {

        if (schemaExists()) {
            return false;
        }

        File tempDir = Files.createTempDirectory("voltdbSchema").toFile();

        if (!tempDir.canWrite()) {
            throw new IOException("Temp Directory (from Files.createTempDirectory()) '" + tempDir.getAbsolutePath()
                    + "' is not writable");
        }

        //
        // Step 1: Load other zip files and classes into a JAR file.
        //
        msg("Creating JAR file in " + tempDir + File.separator + jarFileName);

        JarOutputStream mainJarFileOutputStream = getJarOutputStream(tempDir + File.separator + jarFileName);

        if (otherClasses != null) {
            for (int i = 0; i < otherClasses.length; i++) {

                String resourceName = "/" + otherClasses[i].replace(".", "/") + ".class";
                InputStream is = getValidatedRelativeInputStream(resourceName);
                msg("processing " + resourceName);
                msg("");
                add(otherClasses[i].replace(".", "/") + ".class", is, mainJarFileOutputStream);
            }
        }

        if (procClassNames != null) {
            for (int i = 0; i < procClassNames.length; i++) {
                String resourceName = "/" + procPackageName.replace(".", "/") + "/" + procClassNames[i] + ".class";
                InputStream is = getValidatedRelativeInputStream(resourceName);
                msg("processing " + resourceName);
                msg("");
 
                add(procPackageName.replace(".", "/") + "/" + procClassNames[i] + ".class", is,
                        mainJarFileOutputStream);
            }
        }
        if (zipFiles != null) {

            for (int i = 0; i < zipFiles.length; i++) {

                String resourceName = "/" + procPackageName.replace(".", "/") + "/" + zipFiles[i];
                InputStream is = getValidatedRelativeInputStream(resourceName);
                msg("processing " + resourceName);
                msg("");
 
                add(procPackageName.replace(".", "/") + "/" + zipFiles[i], is, mainJarFileOutputStream);

            }
        }

        mainJarFileOutputStream.close();
        File newJarFile = new File(tempDir + File.separator + jarFileName);

        //
        // Step 2: Once we know how big the JAR file see if it will work with
        // UpdateClasses...
        //
        if (newJarFile.length() > (VoltPort.MAX_MESSAGE_LENGTH * 0.9)) {
            throw new CreatedFileTooBigException("Payload file " + newJarFile.getName() + " is too big at "
                    + newJarFile.length() + "; max length is " + (VoltPort.MAX_MESSAGE_LENGTH * 0.9));
        }

        //
        // Step 3: Load the JAR file we created earlier into VoltDB...
        //
        byte[] jarFileContents = loadFileIntoByteArray(newJarFile);

        msg("Calling @UpdateClasses to load JAR file containing procedures");

        callUpdateClasses(jarFileContents);

        if (deleteFiles) {
            newJarFile.delete();
        }

        //
        // Step 4: Create tables etc
        //
        for (int i = 0; i < ddlStatements.length; i++) {
            try {
                msg(ddlStatements[i]);
                msg("");
                ClientResponse cr = voltClient.callProcedure("@AdHoc", ddlStatements[i]);
                if (cr.getStatus() != ClientResponse.SUCCESS) {
                    throw new Exception("Attempt to execute '" + ddlStatements[i] + "' failed:" + cr.getStatusString());
                }
            } catch (Exception e) {

                if (e.getMessage().indexOf("object name already exists") > -1) {
                    // Someone else has done this...
                    return false;
                }

                throw new FailedToCreateDDLException(e.getMessage());
            }
        }

        //
        // Step 5: Create procedures
        //
        for (int i = 0; i < procStatements.length; i++) {
            msg(procStatements[i]);
            msg("");
            ClientResponse cr = voltClient.callProcedure("@AdHoc", procStatements[i]);
            if (cr.getStatus() != ClientResponse.SUCCESS) {
                throw new FailedToCreateDDLException(
                        "Attempt to execute '" + procStatements[i] + "' failed:" + cr.getStatusString());
            }
        }

        return schemaExists();

    }

    /**
     * Return an input stream, but make sure it's a real one...
     * 
     * @param pathname
     * @return A valid input stream
     * @throws MissingResourceException
     */
    private InputStream getValidatedRelativeInputStream(String pathname) throws MissingResourceException {
        InputStream is = getClass().getResourceAsStream(pathname);

        if (is == null) {
            throw new MissingResourceException(pathname);
        }

        return is;

    }

    /**
     * Call UpdateClasses for the byte[] 'payload'
     * 
     * @param payload
     * @throws FailedToUpdateClassesException
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    private void callUpdateClasses(byte[] payload)
            throws FailedToUpdateClassesException, NoConnectionsException, IOException, ProcCallException {

        ClientResponse cr = voltClient.callProcedure("@UpdateClasses", payload, null);
        if (cr.getStatus() != ClientResponse.SUCCESS) {
            throw new FailedToUpdateClassesException("Attempt to execute UpdateClasses failed:" + cr.getStatusString());
        }

    }

    /**
     * Load a file into a byte array
     * 
     * @param file
     * @return byte[] containing contents of file
     * @throws IOException
     */
    private byte[] loadFileIntoByteArray(File file) throws IOException {

        byte[] fileContents = new byte[(int) file.length()];
        FileInputStream fis = new FileInputStream(file);
        fis.read(fileContents);
        fis.close();
        return fileContents;

    }

    /**
     * Method to take an array of "CREATE PROCEDURE" statements and return a
     * list of the class files they are talking about.
     * 
     * @param procStatements
     * @return a list of the class files they are talking about.
     */
    private String[] getProcClassNames(String[] procStatements) {

        ArrayList<String> jarFileAL = new ArrayList<String>();

        for (int i = 0; i < procStatements.length; i++) {

            String thisProcStringNoNewLines = procStatements[i].toUpperCase().replace(System.lineSeparator(), " ");

            if (thisProcStringNoNewLines.indexOf(" FROM CLASS ") > -1) {
                String[] thisProcStringAsWords = procStatements[i].replace(".", " ").split(" ");

                if (thisProcStringAsWords[thisProcStringAsWords.length - 1].endsWith(";")) {
                    jarFileAL.add(thisProcStringAsWords[thisProcStringAsWords.length - 1].replace(";", ""));

                } else {
                    error("Parsing of '" + procStatements[i] + "' went wrong; can't find proc name");
                }

            }
        }

        String[] jarFileList = new String[jarFileAL.size()];
        jarFileList = jarFileAL.toArray(jarFileList);

        return jarFileList;
    }

    /**
     * Add an entry to our JAR file.
     * 
     * @param fileName
     * @param source
     * @param target
     * @throws IOException
     */
    private void add(String fileName, InputStream source, JarOutputStream target) throws IOException {
        BufferedInputStream in = null;
        try {

            JarEntry entry = new JarEntry(fileName.replace("\\", "/"));
            entry.setTime(System.currentTimeMillis());
            target.putNextEntry(entry);
            in = new BufferedInputStream(source);

            byte[] buffer = new byte[1024];
            while (true) {
                int count = in.read(buffer);
                if (count == -1) {
                    break;
                }

                target.write(buffer, 0, count);
            }
            target.closeEntry();
        } finally {
            if (in != null) {
                in.close();
            }

        }
    }

    /**
     * See if we think Schema already exists...
     * 
     * @return true if the 'Get' procedure exists and takes one string as a
     *         parameter.
     */
    public boolean schemaExists() {

        boolean schemaExists = false;

        try {
            ClientResponse response = voltClient.callProcedure(testProcName, testParams);

            if (response.getStatus() == ClientResponse.SUCCESS) {
                // Database exists...
                schemaExists = true;
            } else {
                // If we'd connected to a copy of VoltDB without the schema and
                // tried to
                // call Get
                // we'd have got a ProcCallException
                error("Error while calling schemaExists(): " + response.getStatusString());
                schemaExists = false;
            }
        } catch (ProcCallException pce) {
            schemaExists = false;

            // Sanity check: Make sure we've got the *right*
            // ProcCallException...
            if (!pce.getMessage().equals(PROCEDURE + testProcName + WAS_NOT_FOUND)) {
                error("Got unexpected Exception while calling schemaExists()" + pce.getMessage());
            }

        } catch (Exception e) {
            error("Error while creating classes." + e.getMessage());
            schemaExists = false;
        }

        return schemaExists;
    }

    /**
     * Take a file name and return a JAR output stream.
     * 
     * @param fileName
     * @return a JAR output stream.
     * @throws FileNotFoundException
     * @throws IOException
     */
    private JarOutputStream getJarOutputStream(String fileName) throws FileNotFoundException, IOException {

        Manifest mainJarManifest = new Manifest();
        mainJarManifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        JarOutputStream mainJarFileOutputStream = new JarOutputStream(new FileOutputStream(fileName), mainJarManifest);

        return mainJarFileOutputStream;

    }

    /**
     * @return true if we delete JAR files after creation.
     */
    public boolean isDeleteFiles() {
        return deleteFiles;
    }

    /**
     * @param false
     *            if we keep files after creation. Default is 'true'
     */
    public void setDeleteFiles(boolean deleteFiles) {
        this.deleteFiles = deleteFiles;
    }

    /**
     * Generic routine to format and print messages.
     * @param message
     */
    public static void msg(String message) {

        Date now = new Date();
        String strDate = sdfDate.format(now);
        System.out.println(strDate + ":" + message);
    }

    /**
     * Generic routine to format and print error messages.
     * @param message
     */
    public static void error(String message) {
        msg("Error: " + message);

    }
 
}
