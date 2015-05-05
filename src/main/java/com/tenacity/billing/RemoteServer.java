package com.tenacity.billing;

import com.google.common.base.Objects;

import java.util.List;
import java.util.Map;

/**
 * Represents a remote server
 */
public class RemoteServer {

    //Server name
    private String name;

    //Server address
    private String address;

    //database username
    private String username = "root";

    //database password
    private String password = "advah310755";

    //database to connect
    private String database = "asteriskcdrdb";

    //port to connect to the database
    private int port = 3306;

    //table to use the application
    private String table = "asteriskcdrdb";

    private List<Map<String,String>> numberMap;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<Map<String, String>> getNumberMap() {
        return numberMap;
    }

    public void setNumberMap(List<Map<String, String>> numberMap) {
        this.numberMap = numberMap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RemoteServer)) return false;
        RemoteServer that = (RemoteServer) o;
        return com.google.common.base.Objects.equal(getPort(), that.getPort()) &&
                com.google.common.base.Objects.equal(getName(), that.getName()) &&
                com.google.common.base.Objects.equal(getAddress(), that.getAddress()) &&
                com.google.common.base.Objects.equal(getUsername(), that.getUsername()) &&
                com.google.common.base.Objects.equal(getPassword(), that.getPassword()) &&
                com.google.common.base.Objects.equal(getDatabase(), that.getDatabase()) &&
                com.google.common.base.Objects.equal(getTable(), that.getTable());
    }

    @Override
    public int hashCode() {
        return com.google.common.base.Objects.hashCode(getName(), getAddress(), getUsername(), getPassword(), getDatabase(), getPort(), getTable());
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("name", name)
                .add("address", address)
                .add("username", username)
                .add("password", password)
                .add("database", database)
                .add("port", port)
                .add("table", table)
                .toString();
    }
}
