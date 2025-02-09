/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.commons.util.profiledb.storage;

import com.google.gson.Gson;
import org.apache.wayang.commons.util.profiledb.ProfileDB;
import org.apache.wayang.commons.util.profiledb.model.Experiment;

import java.io.*;
import java.net.URI;
import java.util.Collection;
import java.util.LinkedList;

/**
 * Controls how conducted experiments will be persisted and loaded
 */
public abstract class Storage {

    /**
     * Object or URI where experiments are persisted
     */
    private URI storageFile;

    /**
     * To access profileDB general serialization functions
     */
    private ProfileDB context;

    /**
     * Creates a new instance.
     * @param uri Object or URI where experiments are persisted
     */
    public Storage(URI uri){
        this.storageFile = uri;
    }



    /**
     * Sets the ProfileDB for this instance that manages all the Measurement subclasses
     * */
    public void setContext(ProfileDB context) {
        this.context = context;
    }

    /**
     * Allows to change where future experiments will be persisted and loaded
     * @param uri
     */
    public void changeLocation(URI uri){
        this.storageFile = uri;
    }

    /**
     * Receive an array of {@link Experiment}s and persist them
     *
     * @param experiments Array of {@link Experiment}s to be persisted
     * @throws IOException
     */
    public abstract void save(Experiment... experiments) throws IOException;

    /**
     * Receive a Collection of {@link Experiment}s and persist them
     *
     * @param experiments Collection of {@link Experiment}s to be persisted
     * @throws IOException
     */
    public abstract void save(Collection<Experiment> experiments) throws IOException;

    /**
     * Related to file based storage, Receive an array of {@link Experiment}s and persist them at the end of a file
     *
     * @param experiments Array of {@link Experiment}s to be persisted
     * @throws IOException
     */
    public abstract void append(Experiment... experiments) throws IOException;

    /**
     * Related to file based storage, Receive a Collection of {@link Experiment}s and persist them at the end of a file
     *
     * @param experiments Collection of {@link Experiment}s to be persisted
     * @throws IOException
     */
    public abstract void append(Collection<Experiment> experiments) throws IOException ;

    /**
     * Bring {@link Experiment}s from current Storage to local variable
     *
     * @return Collection of {@link Experiment}s
     * @throws IOException
     */
    public abstract Collection<Experiment> load() throws IOException;


    //TODO The following methods should be moved to file storage implementation
    /**
     * Write {@link Experiment}s to an {@link OutputStream}.
     *
     * @param outputStream the {@link OutputStream}
     */
    public void save(Collection<Experiment> experiments, OutputStream outputStream) throws IOException {
        try {
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"));
            this.save(experiments, writer);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Unexpectedly, UTF-8 is not supported.");
        }
    }

    /**
     * Write {@link Experiment}s to a {@link Writer}.
     *
     * @param writer the {@link Writer}
     */
    public void save(Collection<Experiment> experiments, Writer writer) throws IOException {
        try {
            Gson gson = context.getGson();
            for (Experiment experiment : experiments) {
                gson.toJson(experiment, writer);
                writer.append('\n');
            }
            writer.flush();
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Unexpectedly, UTF-8 is not supported.");
        }
    }

    /**
     * Load {@link Experiment}s from an {@link InputStream}.
     *
     * @param inputStream the {@link InputStream}
     * @return the {@link Experiment}s
     */
    public Collection<Experiment> load(InputStream inputStream) throws IOException {
        try {
            return load(new BufferedReader(new InputStreamReader(inputStream, "UTF-8")));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Unexpectedly, UTF-8 is not supported.");
        }
    }

    /**
     * Load {@link Experiment}s from an {@link Reader}.
     *
     * @param reader the {@link Reader}
     * @return the {@link Experiment}s
     */
    public Collection<Experiment> load(BufferedReader reader) throws IOException {
        Collection<Experiment> experiments = new LinkedList<>();
        Gson gson = context.getGson();
        String line;
        while ((line = reader.readLine()) != null) {
            Experiment experiment = gson.fromJson(line, Experiment.class);
            experiments.add(experiment);
        }
        return experiments;
    }
}
